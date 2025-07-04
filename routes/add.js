const express = require('express');
const router = express.Router();
const redis = require('../redisClient');

router.post('/', async (req, res) => {
  const { sellerid } = req.body;

  if (!sellerid) return res.status(400).send('Falta sellerid');

  try {
    await redis.sAdd('sellersactivos', sellerid);
    res.send(`✅ Seller ${sellerid} agregado`);
  } catch (err) {
    res.status(500).send(err.message);
  }
});

module.exports = router;