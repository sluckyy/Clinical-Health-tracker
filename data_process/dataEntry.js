var winston = require('winston')
const {format, createLogger} = require('winston')
const { combine, timestamp, prettyPrint } = format

const logger = createLogger({
  format: combine(
    timestamp(),
    prettyPrint()
  ),
  transports: [
    new winston.transports.File({ filename: '../logs/dataProcess_error.log' })
  ],
  exceptionHandlers: [
    new winston.transports.File({ filename: '../logs/dataProcess_exceptions.log' })
  ]
})

// set up corn job for data process
try {
  logger.log('info', 'log success')
  const job = require('./cornjob.js')
} catch (err) {
  logger.error(err)
}
