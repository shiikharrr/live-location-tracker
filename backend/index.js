require ('dotenv').config();

const express = require('express');
const http = require('http');
const {Server} = require('socket.io');
const cors = require('cors');

const  { verfifySocketToken } = require('./auth/middleware');
const  { publishLocation } = require('./kafka/producer');
const  { startLocationConsumer } = require('./kafka/consumer');
const  { startDbProcessor } = require('./kafka/dbProcessor');

const app = express();
app.use(cors());
app.use(express.json());

const httpServer = http.createServer(app);

const io = new Server(httpServer, {
    cors: {
        origin: '*',
        methods: ['GET', 'POST']
    }
});

app.get('/', (req, res) => {
    res.json({ status: 'Live Location Tracker Backend is running!'})
});

io.use(verifySocketToken);

io.on('connection', (socket) => {
    console.log(`User connected: ${socket.userId} (socket: ${socket.id})`);

    socket.on('send-location', async (data) => {
        if (
            typeof data.latitude !== 'number' ||
            typeof data.longitude !== 'number' 
        ) {
            console.warn(`Invalid location data from ${socket.userId}`);
            return;
        }

        const locationEvent = {
            userId: socket.userId,
            latitude: data.latitude,
            longitude: data.longitude,
            timestamp: new Date().toISOString()
        };

        console.log(`Location received from ${socket.userId }:`, locationEvent);

        await publishLocation(locationEvent);
    });

    socket.on('disconnect', () => {
        console.log(`User disconnected: ${socket.userId}`);

        io.emit('user-left', {userId: socket.userId});
    });
});

const PORT = process.env.PORT || 3000;

httpServer.listen(PORT, async () => {
    console.log(`Server running on port ${PORT}`);

    await startLocationConsumer(io);
    await startDbProcessor();

});