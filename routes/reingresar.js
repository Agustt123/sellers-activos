// routes/publicarMensaje.js
const express = require('express');
const { sendMessage } = require('../controllers/cargarEnviodevuelta');
const router = express.Router();


router.post('/publicar', async (req, res) => {
    const { orders, shipments, sellerid } = req.body;

    if (!sellerid) {
        return res.status(400).json({ error: "Falta el parámetro sellerid." });
    }

    let resource = null;

    if (orders) {
        resource = `/orders/${orders}`;
    } else if (shipments) {
        resource = `/shipments/${shipments}`;
    } else {
        return res.status(400).json({ error: "Debe incluir 'orders' o 'shipments' en el cuerpo." });
    }

    try {
        await sendMessage({ resource, sellerid });
        res.status(200).json({ success: true, message: 'Mensaje enviado correctamente.', resource });
    } catch (error) {
        console.error("❌ Error al enviar mensaje:", error);
        res.status(500).json({ success: false, message: 'Error al enviar mensaje.', error: error.message });
    }
});

module.exports = router;
