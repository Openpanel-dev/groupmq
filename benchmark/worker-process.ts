#!/usr/bin/env node

import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import * as BullMQ from 'bullmq';
import Redis from 'ioredis';
import * as GroupMQ from '../src/index';

// Job workloads - copied from main benchmark
async function cpuIntensiveJob(): Promise<void> {
  // PBKDF2 to simulate CPU load (increased iterations for longer processing)
  const salt = crypto.randomBytes(16);
  crypto.pbkdf2Sync('benchmark-job', salt, 200000, 64, 'sha512');
}

async function ioIntensiveJob(): Promise<void> {
  // File operations to simulate I/O load
  const tmpFile = path.join(
    '/tmp',
    `benchmark-${crypto.randomBytes(8).toString('hex')}`,
  );
  const data = crypto.randomBytes(64 * 1024); // 64KB

  await fs.promises.writeFile(tmpFile, data);
  await fs.promises.readFile(tmpFile);
  await fs.promises.unlink(tmpFile);
}

// Parse command line arguments
const args = process.argv.slice(2);
const config = {
  mq: args[0] as 'bullmq' | 'groupmq',
  namespace: args[1],
  jobType: args[2] as 'cpu' | 'io',
  workerId: parseInt(args[3], 10),
};

console.log(
  `🔧 Worker ${config.workerId} starting (${config.mq}, ${config.jobType})`,
);

const jobHandler = config.jobType === 'cpu' ? cpuIntensiveJob : ioIntensiveJob;

// Start worker based on queue type
async function startWorker() {
  const redis = new Redis({
    host: 'localhost',
    port: 6379,
    maxRetriesPerRequest: null,
  });

  if (config.mq === 'bullmq') {
    const worker = new BullMQ.Worker(
      config.namespace,
      async (job) => {
        const startTime = Date.now();
        const enqueuedAt = job.data.enqueuedAt;

        const processingStart = performance.now();
        await jobHandler();
        const processingEnd = performance.now();

        const pickupMs = startTime - enqueuedAt;
        const processingMs = processingEnd - processingStart;

        // Log completion with proper timing for parent process to track
        console.log(
          `COMPLETED:${job.id}:${enqueuedAt}:${startTime}:${Date.now()}:${pickupMs}:${processingMs}`,
        );
      },
      {
        connection: redis,
        concurrency: 1,
      },
    );

    worker.on('error', (err) => {
      console.error('Worker error:', err);
    });

    console.log(`✅ BullMQ Worker ${config.workerId} ready`);
  } else if (config.mq === 'groupmq') {
    const queue = new GroupMQ.Queue({ redis, namespace: config.namespace });
    const worker = new GroupMQ.Worker({
      queue,
      name: `worker-${config.workerId}`,
      handler: async (job) => {
        const startTime = Date.now();
        const enqueuedAt = job.data.enqueuedAt;

        const processingStart = performance.now();
        await jobHandler();
        const processingEnd = performance.now();

        const pickupMs = startTime - enqueuedAt;
        const processingMs = processingEnd - processingStart;

        // Log completion with proper timing for parent process to track
        console.log(
          `COMPLETED:${job.id}:${enqueuedAt}:${startTime}:${Date.now()}:${pickupMs}:${processingMs}`,
        );
      },
      atomicCompletion: true,
    });

    worker.on('error', (err) => {
      console.error('Worker error:', err);
    });

    console.log(`✅ GroupMQ Worker ${config.workerId} ready`);
    await worker.run();
  }
}

// Graceful shutdown
process.on('SIGINT', () => {
  console.log(`🛑 Worker ${config.workerId} shutting down`);
  process.exit(0);
});

process.on('SIGTERM', () => {
  console.log(`🛑 Worker ${config.workerId} shutting down`);
  process.exit(0);
});

// Start the worker
startWorker().catch((err) => {
  console.error('Worker failed:', err);
  process.exit(1);
});
