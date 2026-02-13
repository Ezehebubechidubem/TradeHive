const { Worker } = require('bullmq');
const redis = require('../config/redis');

const worker = new Worker(
  'jobQueue',
  async (job) => {
    console.log('Processing job:', job.name);

    if (job.name === 'notifyTechnician') {
      console.log('Notifying technician:', job.data);
    }

    if (job.name === 'releasePayment') {
      console.log('Releasing payment:', job.data);
    }
  },
  { connection: redis }
);

console.log('Worker started...');
