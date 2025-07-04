const fs = require('fs');

function registrarCambioSeller(accion, sellerId) {
  const fecha = new Date().toISOString();
  const linea = `${fecha} - ${accion} seller ID: ${sellerId}\n`;

  const archivo = accion === 'AGREGADO' ? 'sellers_agregados.txt' : 'sellers_eliminados.txt';
  fs.appendFileSync(archivo, linea);
}

module.exports = registrarCambioSeller;