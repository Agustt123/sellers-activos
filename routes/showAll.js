const express = require('express');
const router = express.Router();
const redis = require('../redisClient');

router.get('/', async (req, res) => {
  try {
    const sellers = await redis.sMembers('sellersactivos');
    res.json(sellers);
  } catch (err) {
    res.status(500).send(err.message);
  }
});

module.exports = router;