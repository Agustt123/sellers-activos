const express = require('express');
const app = express();
const PORT = 13000;

app.use(express.json());

app.use('/sellersactivos/add', require('./routes/add')); //probado 
app.use('/sellersactivos/delete', require('./routes/remove')); //probado
app.use('/sellersactivos/showall', require('./routes/showAll')); //probado
app.use('/sellersactivos/addV2', require('./routes/addV2')); //probado 
app.use('/sellersactivos/showallV2', require('./routes/showAllV2'));//probado
app.use('/sellersactivos/deleteAllV2', require('./routes/deleteAllV2')); //probado
app.use('/sellersactivos/reingresar', require('./routes/reingresar')); //probado
app.use('/callback', require('./routes/incomes')); //probado


app.listen(PORT, () => {
  console.log(`✅ Servidor corriendo en puerto ${PORT}`);
});