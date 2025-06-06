// index.js
require("dotenv").config();

const {
  Kafka,
  logLevel,
  CompressionTypes,
  CompressionCodecs,
} = require("kafkajs");
const { SchemaRegistry } = require("@kafkajs/confluent-schema-registry");
const avro    = require("avsc");
const util    = require("util");
const SnappyCodec = require("kafkajs-snappy");

// ─── REGISTER SNAPPY ─────────────────────────────────────────────────────────
// This replaces KafkaJS’s built-in stub (which throws “not implemented”) with
// the real Snappy codec from kafkajs-snappy. Do this before you create any
// producer/consumer instances:

CompressionCodecs[CompressionTypes.Snappy] = SnappyCodec;

// ── Constants ───────────────────────────────────────────────────────
const CONSUMER_CONFIG = {
  sessionTimeout: 30000,
  rebalanceTimeout: 60000,
  heartbeatInterval: 3000,
  maxWaitTimeInMs: 60000,
  retry: {
    retries: 3,
    initialRetryTime: 1000,
    maxRetryTime: 30000
  }
};

const MESSAGE_TIMEOUT = 120000; // 2 minutes

// ── Environment Setup ───────────────────────────────────────────────
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
  .filter(([_, value]) => !value)
  .map(([key]) => key);

if (missing.length > 0) {
  console.error(`❌ Missing environment variables: ${missing.join(', ')}`);
  process.exit(1);
}

const { 
  KAFKA_ENDPOINT: kafkaEndpoint,
  KAFKA_SECRET: kafkaSecret,
  KAFKA_KEY: kafkaKey,
  KAFKA_TOPIC: kafkaTopic,
  SCHEMA_REGISTRY_URL: schemaRegistryUrl,
  SCHEMA_REGISTRY_KEY: schemaRegistryKey,
  SCHEMA_REGISTRY_SECRET: schemaRegistrySecret
} = required;

// ── Clients ─────────────────────────────────────────────────────────
const kafka = new Kafka({
  clientId: "latest-message-consumer",
  brokers: [kafkaEndpoint],
  ssl: true,
  sasl: {
    mechanism: "plain",
    username: kafkaKey,
    password: kafkaSecret,
  },
  connectionTimeout: 30000,
  requestTimeout: 30000,
  logLevel: logLevel.ERROR,
});

const registry = new SchemaRegistry({
  host: schemaRegistryUrl,
  auth: {
    username: schemaRegistryKey,
    password: schemaRegistrySecret,
  },
});

const schemaCache = new Map();

// ── Functions ───────────────────────────────────────────────────────
/**
 * Verifies that the specified Kafka topic exists and has partitions
 * @throws {Error} If topic doesn't exist or has no partitions
 */
async function verifyTopicExists() {
  const admin = kafka.admin();
  try {
    await admin.connect();
    const metadata = await admin.fetchTopicMetadata({ topics: [kafkaTopic] });
    if (!metadata.topics.length || !metadata.topics[0].partitions.length) {
      throw new Error(`Topic "${kafkaTopic}" not found or has no partitions.`);
    }
  } finally {
    await admin.disconnect().catch(() => {});
  }
}

/**
 * Decodes an Avro message using the schema registry
 * @param {Buffer} buffer - The message buffer to decode
 * @returns {Promise<Object>} The decoded message
 */
async function decodeMessage(buffer) {
  const schemaId = buffer.slice(1, 5).readUInt32BE(0);
  const payload = buffer.slice(5);

  let type = schemaCache.get(schemaId);
  if (!type) {
    const { schema } = await registry.getSchema(schemaId);
    type = avro.Type.forSchema(JSON.parse(schema));
    schemaCache.set(schemaId, type);
  }

  return type.fromBuffer(payload);
}

/**
 * Retrieves the latest message from the Kafka topic
 * @throws {Error} If unable to retrieve the message
 */
async function getLatestMessage() {
  const admin = kafka.admin();
  let offsets;
  let consumer;

  try {
    await admin.connect();
    offsets = await admin.fetchTopicOffsets(kafkaTopic);
    console.log('🔍 Found topic offsets:', offsets);
  } finally {
    await admin.disconnect().catch(console.error);
  }

  const { partition, offset } = offsets.find((o) => o.partition === 0) || {};
  if (!offset) {
    throw new Error('No messages found in topic');
  }

  const lastOffsetNum = Math.max(0, Number(offset) - 1);
  const lastOffset = lastOffsetNum.toString();
  console.log(`📖 Attempting to read message at offset ${lastOffset}`);

  // Generate unique group ID with timestamp and random string
  const uniqueGroupId = `latest-message-group-${Date.now()}-${Math.random().toString(36).substring(7)}`;
  
  consumer = kafka.consumer({ 
    groupId: uniqueGroupId,
    ...CONSUMER_CONFIG
  });

  let resolveMessage, rejectMessage;
  const gotOne = new Promise((resolve, reject) => {
    resolveMessage = resolve;
    rejectMessage = reject;
  });

  try {
    await consumer.connect();
    console.log('✅ Consumer connected');

    // Subscribe before setting up the group join handler
    await consumer.subscribe({ topic: kafkaTopic, fromBeginning: false });

    consumer.on(consumer.events.GROUP_JOIN, async () => {
      try {
        console.log(`🎯 Seeking to offset ${lastOffset}`);
        await consumer.seek({ topic: kafkaTopic, partition, offset: lastOffset });
      } catch (err) {
        console.error('❌ Seek error:', err);
        rejectMessage(err);
      }
    });

    // Add connection error handler
    consumer.on(consumer.events.CONNECT, () => {
      console.log('📡 Consumer reconnected');
    });

    // Add disconnect handler
    consumer.on(consumer.events.DISCONNECT, () => {
      console.log('🔌 Consumer disconnected');
    });

    await consumer.run({
      eachMessage: async ({ topic, partition: p, message }) => {
        let decoded;
        try {
          decoded = await registry.decode(message.value);
        } catch (decodeErr) {
          console.error("❌ Failed to decode Avro message:", decodeErr.message);
          decoded = { raw: message.value.toString("base64") };
        }

        console.log(
          "✅ Latest message:",
          util.inspect(
            {
              topic,
              partition: p,
              offset: message.offset,
              decoded,
            },
            { depth: null, colors: true }
          )
        );

        resolveMessage();
      }
    });

    // Increase timeout to 2 minutes
    const timeout = setTimeout(() => {
      const error = new Error('Timeout waiting for message');
      console.error('❌ Consumer timeout:', error);
      rejectMessage(error);
    }, MESSAGE_TIMEOUT);

    await gotOne;
    clearTimeout(timeout);

  } catch (error) {
    console.error('❌ Consumer error:', error);
    throw error;
  } finally {
    if (consumer) {
      try {
        // Leave the consumer group before disconnecting
        await consumer.stop();
        await consumer.disconnect();
        console.log('👋 Consumer cleanup completed');
      } catch (error) {
        console.error('❌ Error during consumer cleanup:', error);
      }
    }
  }
}

// Handle graceful shutdown
process.on('SIGTERM', async () => {
  console.log('🛑 Received SIGTERM. Starting graceful shutdown...');
  try {
    // Add any cleanup logic here
    process.exit(0);
  } catch (error) {
    console.error('❌ Error during shutdown:', error);
    process.exit(1);
  }
});

// ── Main ────────────────────────────────────────────────────────────
(async () => {
  try {
    await verifyTopicExists();
    await getLatestMessage();
  } catch (error) {
    console.error("❌ Fatal error:", error.stack);
    process.exit(1);
  }
})();
