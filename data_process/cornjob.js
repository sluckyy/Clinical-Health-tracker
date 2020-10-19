const process = require('./data_process')
var schedule = require('node-schedule')
var winston = require('winston')
const {format, createLogger} = require('winston')
const {combine, timestamp, prettyPrint} = format
// const myFormat = printf(info => {
//   return `${info.timestamp} ${info.level}: ${info.message}`
// })
const logger = createLogger({
  format: combine(
    timestamp(),
    prettyPrint()
  ),
  transports: [
    new winston.transports.File({ filename: '../logs/job.log' })
  ],
  exceptionHandlers: [
    new winston.transports.File({ filename: '../logs/job_exceptions.log' })
  ]
})

console.log('Before job instantiation')
logger.log('info', 'log success')
// const job = schedule.scheduleJob('00 00 00 * * 1-7', function () {
// const job = schedule.scheduleJob('* * * * * *', function () {
  try {
    process.updateAllUsers()
    // process.UserBasedUpdate('')
    const d = new Date()
    logger.log('info', 'success onTick:', d)
  } catch (err) {
    logger.error(err)
  }
// })
console.log('After job instantiation')
