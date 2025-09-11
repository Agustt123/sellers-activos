const nodemailer = require("nodemailer");

const transporter = nodemailer.createTransport({
    service: "gmail",
    auth: {
        user: "agustintracheskyoficial@gmail.com",
        pass: "nurmhmvfcndsfbti",
    },
});

async function enviarAlertaPorCorreo(asunto, mensaje) {
    try {
        await transporter.sendMail({
            from: '"Sistema Callback" <agustintracheskyoficial@gmail.com>',
            to: "agustintracheskyoficial@gmail.com", // O cualquier otro correo de destino
            subject: asunto,
            text: mensaje,
        });
        console.log("📧 Alerta enviada por mail.");
    } catch (err) {
        console.error("❌ Error al enviar correo:", err.message);
    }
}

module.exports = {
    enviarAlertaPorCorreo,
};