const express = require('express');
const router = express.Router();
const redis = require('../redisClient');
const registrarCambioSeller = require('../helpers/logCambio');

router.post('/', async (req, res) => {
  const { sellerid, data } = req.body;

  if (!sellerid || !data) return res.status(400).send('Faltan datos');

  try {
    const key = 'sellersactivosV2';
    const clave = `${data.didEmpresa}-${data.didCliente}-${data.didCuenta}`;
    const nuevo = {
      didEmpresa: data.didEmpresa,
      didCliente: data.didCliente,
      didCuenta: data.didCuenta,
      me1: data.me1,
      ff: data.ff,
      ia: data.ia,
      clave
    };

    let arrayActual = [];

    if (await redis.hExists(key, sellerid)) {
      const actual = await redis.hGet(key, sellerid);
      arrayActual = JSON.parse(actual);
      if (arrayActual.some(d => d.clave === clave)) {
        return res.send('🔁 Clave ya existente');
      }
    }

    arrayActual.push(nuevo);
    await redis.hSet(key, sellerid, JSON.stringify(arrayActual));
    await redis.sAdd('sellersactivos', sellerid);
    registrarCambioSeller('AGREGADO', sellerid);

    res.send(`✅ Seller ${sellerid} agregado con datos V2`);
  } catch (err) {
    res.status(500).send(err.message);
  }
});

module.exports = router;