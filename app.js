#!/usr/bin/env node
require('dotenv').config({ path: `${__dirname}/.env` });
const compression = require('compression');
const mongoose = require('mongoose');
const morgan = require('morgan');
const express = require('express');
const route = require('./src/route');

morgan.token('host', (req) => req.headers.host);
morgan.token('worker', () => process.pid);

const app = express();

const PORT = process.env.PORT || 3010;

/* MLAB */
const mongoDB = process.env.MONGODB_URL;
const options = {
    keepAlive: true,
    keepAliveInitialDelay: 300000,
    connectTimeoutMS: 30000,
    autoIndex: false,
    socketTimeoutMS: 30000,
};

if (process.env.MONGODB_USER) {
    options.user = process.env.MONGODB_USER;
    options.pass = process.env.MONGODB_PASSWORD;
}

if (process.env.MONGODB_AUTHDB) {
    options.authSource = process.env.MONGODB_AUTHDB;
}

mongoose.set('strictQuery', false);

mongoose.connect(mongoDB, options);
mongoose.Promise = global.Promise;
const db = mongoose.connection;
db.on('error', (err) => console.error(err));
/* MLAB */

/* REST CONFIG */
app.set('view engine', 'ejs');
app.set('trust proxy', true);
app.use(express.urlencoded({ extended: false, parameterLimit: 100000, limit: '100mb' }));
app.use(express.json({ extended: false, parameterLimit: 100000, limit: '100mb' }));
app.use(compression());
app.use(morgan('[:worker] :remote-addr (:user-agent) :host - :method :url HTTP/:http-version :status - :res[content-length] bytes - :response-time[0] ms'));
/* REST CONFIG */

/* ROUTES */
app.use('/', route);
/* ROUTES */

/* MONGODB */
db.on('connected', () => {
    console.log('✅ Conectado a MongoDB Atlas');
});

mongoose.connection.on('error', (err) => {
    console.error('❌ Error de conexión a MongoDB:', err);
});
/* MONGODB */

app.listen(PORT, () => console.info(`ReachOut Exercise listening on port ${PORT} and environment ${process.env.NODE_ENV}! - Worker ${process.pid}`));