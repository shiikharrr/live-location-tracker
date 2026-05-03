// backend/kafka/dbProcessor.js
// Second consumer — reads same Kafka topic and saves to database
// This is a SEPARATE consumer group, so it gets its own copy of every message

const { Kafka } = require('kafkajs');
const { Pool } = require('pg'); // PostgreSQL client

const kafka = new Kafka({
    clientId: 'location-tracker-db-processor',
    brokers: [process.env.KAFKA_BROKER],
    ssl: true,
    sasl: {
        mechanism: 'scram-sha-256',
        username: process.env.KAFKA_USERNAME,
        password: process.env.KAFKA_PASSWORD
    }
});

// Different consumer group = gets its own independent stream of messages
const consumer = kafka.consumer({ groupId: 'db-writer' });

// PostgreSQL connection pool
const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false } // Required for cloud DBs like Supabase
});

// Batch write strategy: collect events and write every 5 seconds
// WHY: Writing to DB on EVERY socket event would be very expensive at scale
// Kafka lets us decouple the real-time broadcast from the slower DB write
let locationBuffer = [];

async function flushToDatabase() {
    if (locationBuffer.length === 0) return; // Nothing to write

    const toWrite = [...locationBuffer]; // Copy and clear immediately
        locationBuffer = [];

    try {
    // Build a batch insert query (much faster than one-by-one inserts)
    const values = toWrite.map((_, i) =>
      `($${i * 4 + 1}, $${i * 4 + 2}, $${i * 4 + 3}, $${i * 4 + 4})`
    ).join(', ');

    const params = toWrite.flatMap(e => [
        e.userId, e.latitude, e.longitude, e.timestamp
    ]);

    await pool.query(
        `INSERT INTO location_history (user_id, latitude, longitude, recorded_at)
        VALUES ${values}`,
        params
    );

    console.log(`Saved ${toWrite.length} location events to database`);
    } catch (err) {
        console.error('Database write failed:', err.message);
    // In production, you'd add retry logic here
    }
}

async function startDbProcessor() {
    await consumer.connect();
        console.log('DB processor consumer connected to Kafka');

    await consumer.subscribe({
        topic: 'location-updates',
    fromBeginning: false
    });

  // Flush buffer to DB every 5 seconds (batch write strategy)
    setInterval(flushToDatabase, 5000);

    await consumer.run({
        eachMessage: async ({ message }) => {
        try {
            const locationData = JSON.parse(message.value.toString());
            locationBuffer.push(locationData); // Add to buffer, don't write yet
        } catch (err) {
            console.error('Error buffering location event:', err.message);
        }
    }
    });
}

module.exports = { startDbProcessor };