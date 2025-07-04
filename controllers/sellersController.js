const redis = require('../redisClient');
const registrarCambioSeller = require('../helpers/logCambio');

async function agregarSeller(body) {
  const { sellerid } = body;
  if (!sellerid) throw new Error('Falta sellerid');
  await redis.sAdd('sellersactivos', sellerid);
  return `✅ Seller ${sellerid} agregado`;
}

async function eliminarSeller(sellerId) {
  await redis.hDel('sellersactivosV2', sellerId);
  const result = await redis.sRem('sellersactivos', sellerId);
  if (result > 0) {
    registrarCambioSeller('ELIMINADO', sellerId);
    return `❌ Seller ${sellerId} eliminado`;
  } else {
    throw new Error('Seller no encontrado');
  }
}

async function obtenerSellers() {
  return await redis.sMembers('sellersactivos');
}

async function agregarSellerV2(body) {
  const { sellerid, data } = body;

  if (!sellerid || !data) throw new Error('Faltan datos');

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
      return '🔁 Clave ya existente';
    }
  }

  arrayActual.push(nuevo);
  await redis.hSet(key, sellerid, JSON.stringify(arrayActual));
  await redis.sAdd('sellersactivos', sellerid);
  registrarCambioSeller('AGREGADO', sellerid);

  return `✅ Seller ${sellerid} agregado con datos V2`;
}

async function obtenerSellersV2() {
  const sellers = await redis.hKeys('sellersactivosV2');
  const result = {};
  for (const id of sellers) {
    const data = await redis.hGet('sellersactivosV2', id);
    result[id] = JSON.parse(data);
  }
  return result;
}

async function eliminarTodos() {
  await redis.del('sellersactivos');
  await redis.del('sellersactivosV2');
  return '🔥 Todos los sellers eliminados';
}

module.exports = {
  agregarSeller,
  eliminarSeller,
  obtenerSellers,
  agregarSellerV2,
  obtenerSellersV2,
  eliminarTodos
};