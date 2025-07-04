const redis = require("redis");

const redisClient = redis.createClient({
  socket: {
    host: "192.99.190.137",
    port: 50301,
  },
  password: "sdJmdxXC8luknTrqmHceJS48NTyzExQg",
});

redisClient.on("error", (err) => {
  console.error("Error al conectar con Redis:", err);
});

(async () => {
  await redisClient.connect();
  console.log("Redis conectado");
})();

module.exports = redisClient; // ✅ exportás el cliente, no el módulo redis
