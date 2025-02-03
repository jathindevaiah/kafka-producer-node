import { Kafka } from "kafkajs";
import dotenv from "dotenv";
import { SchemaRegistry } from "@kafkajs/confluent-schema-registry";
import axios from "axios";

dotenv.config();

// Set up Kafka configuration
const kafka = new Kafka({
  clientId: process.env.KAFKA_CLIENT_ID, // Replace with your app name
  brokers: [process.env.KAFKA_BROKER], // Replace with your Confluent Cloud broker(s)
  ssl: true,
  sasl: {
    mechanism: "plain",
    username: process.env.KAFKA_API_KEY, // Replace with your API key
    password: process.env.KAFKA_API_SECRET, // Replace with your API secret
  },
});

// Create producer
const producer = kafka.producer();

// Confluent Schema Registry URL
const registry = new SchemaRegistry({
  host: process.env.KAFKA_SR_URL, // Schema Registry URL
  auth: {
    username: process.env.KAFKA_SR_API_KEY, // API key
    password: process.env.KAFKA_SR_API_SECRET, // API secret
  },
});

//Test SIC message matching the schema
const sicMessage = {};

//Test SII message matching the schema
const siiMessage = {};

const run = async () => {
  await producer.connect();

  try {
    // Fetch the latest version of the schema (it should already be registered)
    // const schemaInfo = await axios.get(`${process.env.KAFKA_SR_URL}/subjects/${process.env.KAFKA_SR_SCHEMA_SUBJECT}/versions/latest`, {
    //   auth: {
    //     username: process.env.KAFKA_SR_API_KEY,
    //     password: process.env.KAFKA_SR_API_SECRET,
    //   },
    // });
    // const schema = schemaInfo.data.schema;

    // Serialize the message using the schema (Avro)
    const serializedMessage = await registry.encode(process.env.KAFKA_SR_SCHEMA_ID, siiMessage);
    console.log("serializedMessage", serializedMessage);

    // Send a message to a Kafka topic
    await producer.send({
      topic: process.env.KAFKA_TOPIC,
      // messages: [{ value: "Hello Kafka from Node.js!" }],
      messages: [
        {
          key: "user-key3",
          value: serializedMessage, // Avro-encoded message
        },
      ],
    });
    console.log("Message sent successfully");
  } catch (error) {
    console.error("Error producing message:", error);
  } finally {
    await producer.disconnect();
  }
};

run().catch(console.error);
