const { Kafka } = require(' kafkajs');

const kafka = new Kafka({
    clientId : 'location-tracker-producer',
    brokers: [process.env.KAFKA_BROKER],
    ssl : true, 
    sasl: {
        mechanism: 'scram-sha-256',
        username: process.env.KAFKA_USERNAME,
        password: process.env.KAFKA_PASSWORD
    }
});

const producer = kafka.producer();
let isConnected = false;

async function connectProducer() {
    if (!isConnected) {
        await producer.connect();
        isConnected = true;
        console.log('Kafka producer connected');
    }
}   

async function publishLocation(locationEvent) {
    try {
        await connectProducer();

        await producer.send({
            topic: 'location-updates',
            messages: [
                {
                    key: locationEvent.userId,  
                    value: JSON.stringify(locationEvent)
                }
            ]
        });

        console.log(`Published to Kafka: ${locationEvent.userId}`);
    } catch (err) {
        console.error('Failed to publish to Kafka:', err.message);
    }
}

module.exports = { publishLocation };