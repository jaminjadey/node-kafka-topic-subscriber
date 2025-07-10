require("dotenv").config();

const {
  Kafka,
  logLevel,
  CompressionTypes,
  CompressionCodecs,
} = require("kafkajs");
const { SchemaRegistry } = require("@kafkajs/confluent-schema-registry");
const util = require("util");
const Big = require("big.js");
const SnappyCodec = require("kafkajs-snappy");

// Register Snappy compression
CompressionCodecs[CompressionTypes.Snappy] = SnappyCodec;

// ─── Decimal Decoder ────────────────────────────────────────────────

function decodeAvroDecimal(buffer, scale = 2) {
  if (!Buffer.isBuffer(buffer)) return null;
  if (buffer.length === 0) return "0";

  const isNegative = (buffer[0] & 0x80) !== 0;
  const unsigned = isNegative ? twosComplement(buffer) : buffer;

  const bigInt = BigInt("0x" + unsigned.toString("hex"));
  const value = isNegative ? -bigInt : bigInt;

  return new Big(value.toString()).div(Math.pow(10, scale)).toString();
}

function twosComplement(buf) {
  const inverted = Buffer.from(buf.map((b) => ~b));
  for (let i = inverted.length - 1; i >= 0; i--) {
    if (++inverted[i] <= 0xff) break;
  }
  return inverted;
}

// Recursively decode all Buffer fields as decimals
function decodeDecimalFields(obj, scale = 2) {
  if (Array.isArray(obj)) {
    return obj.map((item) => decodeDecimalFields(item, scale));
  }
  if (Buffer.isBuffer(obj)) {
    return decodeAvroDecimal(obj, scale);
  }
  if (obj !== null && typeof obj === "object") {
    return Object.fromEntries(
      Object.entries(obj).map(([key, val]) => [
        key,
        decodeDecimalFields(val, scale),
      ])
    );
  }
  return obj;
}

// ─── Config & Setup ─────────────────────────────────────────────────

const CONSUMER_CONFIG = {
  sessionTimeout: 30000,
  rebalanceTimeout: 60000,
  heartbeatInterval: 3000,
  maxWaitTimeInMs: 60000,
  retry: { retries: 3, initialRetryTime: 1000, maxRetryTime: 30000 },
};

const MESSAGE_TIMEOUT = 120000;

const required = {
  KAFKA_ENDPOINT: process.env.KAFKA_ENDPOINT?.trim(),
  KAFKA_SECRET: process.env.KAFKA_SECRET?.trim(),
  KAFKA_KEY: process.env.KAFKA_KEY?.trim(),
  KAFKA_TOPIC: process.env.KAFKA_TOPIC?.trim(),
  SCHEMA_REGISTRY_URL: process.env.SCHEMA_REGISTRY_URL?.trim(),
  SCHEMA_REGISTRY_KEY: process.env.SCHEMA_REGISTRY_KEY?.trim(),
  SCHEMA_REGISTRY_SECRET: process.env.SCHEMA_REGISTRY_SECRET?.trim(),
};

const missing = Object.entries(required)
  .filter(([, v]) => !v)
  .map(([k]) => k);
if (missing.length > 0) {
  console.error(`❌ Missing environment variables: ${missing.join(", ")}`);
  process.exit(1);
}

const {
  KAFKA_ENDPOINT,
  KAFKA_SECRET,
  KAFKA_KEY,
  KAFKA_TOPIC,
  SCHEMA_REGISTRY_URL,
  SCHEMA_REGISTRY_KEY,
  SCHEMA_REGISTRY_SECRET,
} = required;

const kafka = new Kafka({
  clientId: "latest-message-consumer",
  brokers: [KAFKA_ENDPOINT],
  ssl: true,
  sasl: { mechanism: "plain", username: KAFKA_KEY, password: KAFKA_SECRET },
  connectionTimeout: 30000,
  requestTimeout: 30000,
  logLevel: logLevel.ERROR,
});

const registry = new SchemaRegistry({
  host: SCHEMA_REGISTRY_URL,
  auth: { username: SCHEMA_REGISTRY_KEY, password: SCHEMA_REGISTRY_SECRET },
});

// ─── Kafka Helpers ─────────────────────────────────────────────────

async function decodeMessage(buffer) {
  // Let the registry decode named types; decimals stay as Buffer
  return registry.decode(buffer);
}

async function verifyTopicExists() {
  const admin = kafka.admin();
  try {
    await admin.connect();
    const metadata = await admin.fetchTopicMetadata({ topics: [KAFKA_TOPIC] });
    if (!metadata.topics.length || !metadata.topics[0].partitions.length) {
      throw new Error(`Topic "${KAFKA_TOPIC}" not found or has no partitions.`);
    }
  } finally {
    await admin.disconnect().catch(() => {});
  }
}

async function getLatestMessage() {
  const admin = kafka.admin();
  let offsets;
  try {
    await admin.connect();
    offsets = await admin.fetchTopicOffsets(KAFKA_TOPIC);
    console.log("🔍 Found topic offsets:", offsets);
  } finally {
    await admin.disconnect().catch(console.error);
  }

  const { partition, offset } = offsets.find((o) => o.partition === 0) || {};
  if (!offset) throw new Error("No messages found in topic");

  const lastOffset = Math.max(0, Number(offset) - 1).toString();
  console.log(`📖 Attempting to read message at offset ${lastOffset}`);

  const consumer = kafka.consumer({
    groupId: `latest-message-group-${Date.now()}-${Math.random()
      .toString(36)
      .substring(7)}`,
    ...CONSUMER_CONFIG,
  });

  let resolveMessage, rejectMessage;
  const gotOne = new Promise((resolve, reject) => {
    resolveMessage = resolve;
    rejectMessage = reject;
  });

  try {
    await consumer.connect();
    await consumer.subscribe({ topic: KAFKA_TOPIC, fromBeginning: false });

    consumer.on(consumer.events.GROUP_JOIN, async () => {
      await consumer.seek({
        topic: KAFKA_TOPIC,
        partition,
        offset: lastOffset,
      });
    });

    await consumer.run({
      eachMessage: async ({ topic, partition: p, message }) => {
        let decodedWithDecimals;
        try {
          const decodedRaw = await decodeMessage(message.value);
          decodedWithDecimals = decodeDecimalFields(decodedRaw, 2);
        } catch (err) {
          console.error("❌ Failed to decode Avro message:", err.stack || err);
          decodedWithDecimals = { raw: message.value.toString("base64") };
        }

        console.log(
          "✅ Latest message:",
          util.inspect(
            {
              topic,
              partition: p,
              offset: message.offset,
              decoded: decodedWithDecimals,
            },
            { depth: null, colors: true }
          )
        );

        resolveMessage();
      },
    });

    const timeout = setTimeout(
      () => rejectMessage(new Error("Timeout waiting for message")),
      MESSAGE_TIMEOUT
    );
    await gotOne;
    clearTimeout(timeout);
  } finally {
    await consumer.stop();
    await consumer.disconnect();
    console.log("👋 Consumer cleanup completed");
  }
}

// ─── Entrypoint ─────────────────────────────────────────────────────

process.on("SIGTERM", async () => {
  console.log("🛑 Received SIGTERM. Shutting down...");
  process.exit(0);
});

(async () => {
  try {
    await verifyTopicExists();
    await getLatestMessage();
  } catch (error) {
    console.error("❌ Fatal error:", error.stack || error.message);
    process.exit(1);
  }
})();
