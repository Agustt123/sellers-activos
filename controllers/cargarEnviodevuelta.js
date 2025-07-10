// publisher.js
const amqp = require('amqplib');

const sendMessage = async (mensaje) => {
    try {
        const connection = await amqp.connect({
            protocol: "amqp",
            hostname: "158.69.131.226",
            port: 5672,
            username: "lightdata",
            password: "QQyfVBKRbw6fBb",
            heartbeat: 30,
        });

        const channel = await connection.createChannel();
        const queue = 'enviosml_ia';

        await channel.assertQueue(queue, { durable: true });

        const buffer = Buffer.from(JSON.stringify(mensaje));
        channel.sendToQueue(queue, buffer, { persistent: true });

        console.log("✅ Mensaje enviado a la cola:", mensaje);

        // No uses process.exit en un entorno Express
        await channel.close();
        await connection.close();
    } catch (err) {
        console.error("❌ Error enviando mensaje:", err);
        throw err;
    }
};

// Exportamos para que el route pueda usarlo
module.exports = { sendMessage };
