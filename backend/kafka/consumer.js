// backend/kafka/consumer.js
// This consumer reads from Kafka and broadcasts to all map viewers via Socket.IO

const { Kafka } = require('kafkajs');

const kafka = new Kafka({
    clientId: 'location-tracker-socket-consumer',
    brokers: [process.env.KAFKA_BROKER],
    ssl: true,
    sasl: {
        mechanism: 'scram-sha-256',
        username: process.env.KAFKA_USERNAME,
        password: process.env.KAFKA_PASSWORD
    }
});

// Consumer group "socket-broadcaster" — only this group broadcasts to sockets
const consumer = kafka.consumer({ groupId: 'socket-broadcaster' });

async function startLocationConsumer(io) {
    await consumer.connect();
    console.log('Socket consumer connected to Kafka');

  // Subscribe to the same topic the producer writes to
    await consumer.subscribe({
        topic: 'location-updates',
        fromBeginning: false // Only process new messages, not old ones
    });

  // Process messages as they arrive
    await consumer.run({
        eachMessage: async ({ message }) => {
        try {
        // Parse the JSON message we stored in producer
            const locationData = JSON.parse(message.value.toString());

        // Broadcast to ALL connected socket clients
        // Every user's browser will receive this and update their map
            io.emit('location-update', locationData);

            console.log(`Broadcasted location update for: ${locationData.userId}`);
        } catch (err) {
            console.error('Error processing Kafka message (socket):', err.message);
        }
    }
    });
}

module.exports = { startLocationConsumer };