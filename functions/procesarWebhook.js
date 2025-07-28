

const amqp = require("amqplib");

// RabbitMQ
const queue = "webhookml";
const rabbitConfig = {
    protocol: "amqp",
    hostname: "158.69.131.226",
    port: 5672,
    username: "lightdata",
    password: "QQyfVBKRbw6fBb",
    clientProperties: {
        connection_name: "callback"
    }
};

let rabbitConnection;
let rabbitChannel;
async function connectRabbit() {
    if (rabbitChannel) return;

    rabbitConnection = await amqp.connect(rabbitConfig);
    rabbitChannel = await rabbitConnection.createChannel();
    await rabbitChannel.assertQueue(queue, { durable: true });
}

async function procesarWebhook(data, res) {
    await connectRabbit();
    rabbitChannel.sendToQueue(queue, Buffer.from(JSON.stringify(data)), {
        persistent: true,
    });
    //res.status(200).send("Webhook recibido");
}

module.exports = {
    procesarWebhook
}