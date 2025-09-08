
const express = require('express');
const { procesarWebhook } = require('../functions/procesarWebhook');
const redisClient = require('../redisClient');
const router = express.Router();


const sellersActivosCache = new Set();


router.post("/incomes", async (req, res) => {
    const data = req.body;

    const incomeuserid = data.user_id ? data.user_id.toString() : "";
    res.status(200).send("Webhook recibido");



    try {
        // 1. Verificar en cache local
        if (sellersActivosCache.has(incomeuserid)) {
            await procesarWebhook(data);
            return;
        }

        // 2. Verificar en Redis externo
        const isActive = await redisClient.sIsMember("sellersactivos", incomeuserid);

        if (isActive) {
            sellersActivosCache.add(incomeuserid); // Guardar en cache local
            await procesarWebhook(data);
        } else {
            // res.status(403).send("Vendedor no activo");
        }
    } catch (err) {
        console.error("❌ Error procesando webhook:", err.message);
        res.status(500).send("Error interno");
    }
});


module.exports = router;