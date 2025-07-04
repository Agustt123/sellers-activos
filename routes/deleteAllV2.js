const express = require('express');
const router = express.Router();
const redis = require('../redisClient');

router.post('/', async (req, res) => {
  const { sellerid } = req.body;

  if (!sellerid) {
    return res.status(400).send('❌ Falta el sellerid en el body');
  }

  try {
    await redis.hDel('sellersactivosV2', sellerid);
    res.send(`✅ Seller ${sellerid} eliminado de sellersactivosV2`);
  } catch (err) {
    res.status(500).send(`❌ Error: ${err.message}`);
  }
});

module.exports = router;
