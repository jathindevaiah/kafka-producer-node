import { Kafka } from 'kafkajs';
import dotenv from 'dotenv';

dotenv.config();

// Set up Kafka configuration
const kafka = new Kafka({
  clientId: process.env.KAFKA_CLIENT_ID, // Replace with your app name
  brokers: [process.env.KAFKA_BROKER], // Replace with your Confluent Cloud broker(s)
  ssl: true,
  sasl: {
    mechanism: 'plain',
    username: process.env.KAFKA_API_KEY, // Replace with your API key
    password: process.env.KAFKA_API_SECRET, // Replace with your API secret
  },
});

// Create producer
const producer = kafka.producer();

const run = async () => {
  await producer.connect();

  try {
    // Send a message to a Kafka topic
    await producer.send({
      topic: 'test-topic', // Replace with your topic name
      messages: [{ value: 'Hello Kafka from Node.js!' }],
    });
    console.log('Message sent successfully');
  } catch (error) {
    console.error('Error producing message:', error);
  } finally {
    await producer.disconnect();
  }
};

run().catch(console.error);
