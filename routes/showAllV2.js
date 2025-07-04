const express = require('express');
const router = express.Router();
const redis = require('../redisClient'); // ahora redis es el cliente real

router.get('/', async (req, res) => {
  try {
    if (!redis.isOpen) await redis.connect();

    const sellers = await redis.hKeys('sellersactivosV2'); // ✅ esto ahora va a funcionar
    const result = {};

    for (const id of sellers) {
      const data = await redis.hGet('sellersactivosV2', id);
      result[id] = JSON.parse(data);
    }

    res.json(result);
  } catch (err) {
    res.status(500).send(err.message);
  }
});

module.exports = router;
