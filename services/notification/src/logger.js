'use strict';

const pino = require('pino');

// JSON logs to stdout, one object per line, same idea as the other services.
module.exports = pino({
  level: process.env.LOG_LEVEL || 'info',
  base: { service: 'notification' },
  timestamp: pino.stdTimeFunctions.isoTime,
  formatters: { level: (label) => ({ level: label }) },
});