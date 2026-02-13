const { Queue } = require('bullmq');
const redis = require('../config/redis');

const jobQueue = new Queue('jobQueue', {
  connection: redis,
});

module.exports = jobQueue;