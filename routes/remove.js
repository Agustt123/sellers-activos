const express = require('express');
const router = express.Router();
const redis = require('../redisClient');
const registrarCambioSeller = require('../helpers/logCambio');

router.post('/', async (req, res) => {
  const sellerId = req.body.sellerId;

  try {
    await redis.hDel('sellersactivosV2', sellerId);
    const result = await redis.sRem('sellersactivos', sellerId);
    if (result > 0) {
      registrarCambioSeller('ELIMINADO', sellerId);
      res.send(`❌ Seller ${sellerId} eliminado`);
    } else {
      res.status(404).send('Seller no encontrado');
    }
  } catch (err) {
    res.status(500).send(err.message);
  }
});

module.exports = router;