const amqp = require("amqplib");
const mysql = require("mysql2");
const redis = require("redis");
const axios = require("axios");
const { enviarAlertaPorCorreo } = require("./mail");

let pLimit;
let retryCount = 0;
const maxRetries = 5;

// Activa/desactiva escritura en la BD secundaria
const DUAL_WRITE = 1

// Inicializar p-limit
async function initializePLimit() {
    const module = await import("p-limit");
    pLimit = module.default;
}

initializePLimit()
    .then(() => {
        initRabbitMQ();
    })
    .catch((err) => {
        console.error("Error al importar p-limit:", err);
    });

// Redis local
const client = redis.createClient({
    socket: {
        path: "/home/callback/ea-podman.d/ea-redis62.callback.01/redis.sock",
        family: 0,
    },
});
client
    .connect()
    .then(() => {
        console.log("✅ Redis local conectado.");
    })
    .catch((err) => {
        console.error("❌ Error al conectar a Redis local:", err.message);
    });

// Redis remoto
const clientFF = redis.createClient({
    socket: {
        host: "192.99.190.137",
        port: 50301,
    },
    password: "sdJmdxXC8luknTrqmHceJS48NTyzExQg",
});
clientFF
    .connect()
    .then(() => {
        console.log("✅ Redis remoto conectado.");
    })
    .catch((err) => {
        console.error("❌ Error al conectar a Redis remoto:", err.message);
    });

// RabbitMQ
const rabbitMQUrl = "amqp://lightdata:QQyfVBKRbw6fBb@158.69.131.226:5672";
const queue = "webhookml";
let rabbitConnection;
let rabbitChannel;
let isConnecting = false;
let rabbitConnectionActive = false;
let hasStartedConsuming = false;

// --------- BD PRIMARIA (la del original que anda) ----------
const con = mysql.createPool({
    host: "bhsws10.ticdns.com",
    user: "callback_u2u3",
    password: "7L35HWuw,8,i",
    database: "callback_incomesML",
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0,
});

// --------- BD SECUNDARIA (la otra que mencionamos) ----------
const con2 = mysql.createPool({
    host: "149.56.182.49",
    port: 44353,
    user: "root",
    password: "4AVtLery67GFEd",
    database: "callback_incomesML",
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0,
});

con.getConnection((err, connection) => {
    if (err) {
        console.error("❌ Error al conectar al pool de MySQL (primaria):", err.message);
    } else {
        console.log("✅ Pool de MySQL (primaria) conectado.");
        connection.release();
    }
});

con2.getConnection((err, connection) => {
    if (err) {
        console.error("❌ Error al conectar al pool de MySQL (secundaria):", err.message);
    } else {
        console.log("✅ Pool de MySQL (secundaria) conectado.");
        connection.release();
    }
});

// ---------- Helpers de insert idempotente ----------
function insertIgnore(pool, tablename, sellerId, resource) {
    return new Promise((resolve) => {
        const sql = `INSERT IGNORE INTO ${tablename} (seller_id, resource) VALUES (?, ?)`;
        pool.getConnection((err, connection) => {
            if (err) {
                console.error("❌ Error obteniendo conexión del pool:", err.message);
                return resolve(false);
            }
            connection.query(sql, [sellerId, resource], (qErr) => {
                if (qErr) {
                    console.error(`❌ Error insertando en ${tablename}:`, qErr.message);
                    connection.release();
                    return resolve(false);
                }
                connection.release();
                console.log(`✅ INSERT IGNORE en ${tablename} OK (${sellerId}, ${resource})`);
                resolve(true);
            });
        });
    });
}

// Inserta siempre en primaria y (si flag activo) también en secundaria
async function insertEnAmbas(tablename, sellerId, resource) {
    const p1 = insertIgnore(con, tablename, sellerId, resource);
    const p2 = DUAL_WRITE ? insertIgnore(con2, tablename, sellerId, resource) : Promise.resolve(true);
    const [ok1, ok2] = await Promise.all([p1, p2]);

    if (!ok1) console.warn("⚠️ Primaria falló en insert.");
    if (DUAL_WRITE && !ok2) console.warn("⚠️ Secundaria falló en insert.");
    return ok1;
}

// Iniciar RabbitMQ
async function initRabbitMQ() {
    if (isConnecting) return;
    isConnecting = true;

    if (rabbitConnection) {
        try {
            await rabbitConnection.close();
            console.log("🔌 Conexión anterior a RabbitMQ cerrada.");
        } catch (e) {
            console.warn("⚠️ Error cerrando la conexión anterior:", e.message);
        }
    }

    try {
        rabbitConnection = await amqp.connect(rabbitMQUrl);
        rabbitConnection.on("error", handleRabbitError);
        rabbitConnection.on("close", handleRabbitClose);
        rabbitChannel = await rabbitConnection.createChannel();
        await rabbitChannel.assertQueue(queue, { durable: true });
        rabbitConnectionActive = true;
        retryCount = 0;
        console.log("✅ Nueva conexión y canal a RabbitMQ establecidos.");

        // Igual que tu original: arranco consumo acá
        if (!hasStartedConsuming) {
            console.log("entrre");
            consumeQueue();
            // (no seteamos hasStartedConsuming = true para mantener tu comportamiento)
        }
    } catch (error) {
        console.error("❌ Error al conectar a RabbitMQ:", error.message);
        retryCount++;
        if (retryCount >= maxRetries) {
            console.error(`❌ Máximo de intentos (${maxRetries}) alcanzado.`);
            restartScript();
        } else {
            setTimeout(initRabbitMQ, 5000);
        }
    } finally {
        isConnecting = false;
    }
}

function handleRabbitClose() {
    console.warn("⚠️ Conexión a RabbitMQ cerrada. Intentando reconectar...");
    rabbitConnectionActive = false;
    retryCount++;
    if (retryCount >= maxRetries) {
        console.error(`❌ Se alcanzó el límite de reconexiones (${maxRetries}).`);
        restartScript();
    } else {
        setTimeout(initRabbitMQ, 5000);
    }
}

function handleRabbitError(err) {
    console.error("❌ Error en RabbitMQ:", err.message);
    rabbitConnectionActive = false;
}

async function ensureRabbitMQConnection() {
    if (!rabbitConnectionActive) {
        await initRabbitMQ();
    }
}

async function enviarMensajeEstadoML(data, cola) {
    try {
        await ensureRabbitMQConnection();
        if (rabbitChannel && rabbitConnectionActive) {
            rabbitChannel.sendToQueue(cola, Buffer.from(JSON.stringify(data)), {
                persistent: true,
            });
        } else {
            console.warn("❗ Conexión a RabbitMQ no activa, no se pudo enviar a:", cola);
        }
    } catch (error) {
        console.error("❌ Error al enviar mensaje a RabbitMQ:", error.message);
    }
}

let cachedSellers = [];

async function processWebhook(data2) {
    const limit = pLimit(100);
    try {
        const incomeuserid = data2.user_id ? data2.user_id.toString() : "";
        const resource = data2.resource;
        const topic = data2.topic;
        let now = new Date();
        now.setHours(now.getHours() - 3);

        let exists = false;

        if (topic === "flex-handshakes") {
            exists = true;
        } else {
            if (cachedSellers.length === 0 || !cachedSellers.includes(incomeuserid)) {
                try {
                    const response = await axios.get(
                        "https://callbackml.lightdata.app/MLProcesar/get/"
                    );
                    if (
                        response.data &&
                        response.data.success &&
                        Array.isArray(response.data.sellers)
                    ) {
                        cachedSellers = response.data.sellers;
                        exists = cachedSellers.includes(incomeuserid);
                    } else {
                        console.warn("⚠️ Respuesta inesperada del endpoint de sellers");
                    }
                } catch (error) {
                    // silent fail
                }
            } else {
                exists = true;
            }
        }

        if (exists) {
            let tablename = "";
            switch (topic) {
                case "orders_v2":
                    console.log("ordenessssssssss");
                    tablename = "db_orders";
                    await enviarMensajeEstadoML(
                        {
                            resource,
                            sellerid: incomeuserid,
                            fecha: now.toISOString().slice(0, 19).replace("T", " "),
                        },
                        "ordenesFF"
                    );
                    break;

                case "shipments":
                    tablename = "db_shipments";
                    const mensajeRA = {
                        resource,
                        sellerid: incomeuserid,
                        fecha: now.toISOString().slice(0, 19).replace("T", " "),
                    };
                    await enviarMensajeEstadoML(mensajeRA, "shipments_states_callback_ml");
                    await enviarMensajeEstadoML(mensajeRA, "enviosml_ia");
                    break;

                case "flex-handshakes":
                    tablename = "db_flex_handshakes";
                    break;
            }

            if (tablename !== "") {
                // ✅ Dual-write idempotente (sin SELECT previo)
                const ok = await insertEnAmbas(tablename, incomeuserid, resource);
                if (!ok) {
                    // Si falla la primaria, mantenemos tu alerta
                    enviarAlertaPorCorreo("Error en MySQL", `Falló insert en ${tablename}`);
                }
            }
        }
    } catch (e) {
        console.error("❌ Error procesando webhook:", e.message);
    }
}

async function consumeQueue() {
    if (!rabbitChannel || !rabbitConnectionActive) {
        console.warn("❗ No hay canal o conexión activa para consumir.");
        return;
    }

    try {
        const limit = pLimit(500);
        await rabbitChannel.consume(queue, async (msg) => {
            if (!msg) return;
            await limit(async () => {
                try {
                    const data = JSON.parse(msg.content.toString());
                    await processWebhook(data);

                    if (rabbitChannel && rabbitConnectionActive) {
                        rabbitChannel.ack(msg);
                    } else {
                        console.warn("⚠️ No se pudo ack: canal no activo.");
                    }
                } catch (e) {
                    console.error("❌ Error procesando mensaje:", e.message);
                    if (rabbitChannel && rabbitConnectionActive) {
                        rabbitChannel.nack(msg, false, false);
                    } else {
                        console.warn("⚠️ No se pudo nack, reiniciando...");
                        restartScript();
                    }
                }
            });
        });
        console.log("✅ Consumo de cola iniciado.");
    } catch (error) {
        console.error("❌ Error en consumeQueue:", error.message);
        setTimeout(consumeQueue, 5000);
    }
}

function restartScript() {
    console.warn("🔄 Reiniciando el script...");
    process.exit(1);
}
