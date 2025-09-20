import { createBullBoard } from '@bull-board/api';
import { HonoAdapter } from '@bull-board/hono';
import { serve } from '@hono/node-server';
import { serveStatic } from '@hono/node-server/serve-static';
import { Hono } from 'hono';
import { showRoutes } from 'hono/dev';
import Redis from 'ioredis';
import { Queue, Worker } from '../src';
import { BullBoardGroupMQAdapter } from '../src/adapters/groupmq-bullboard-adapter';

const sleep = (t) => new Promise((resolve) => setTimeout(resolve, t));

const redis = new Redis('redis://localhost:6379', {
  maxRetriesPerRequest: null,
});

const run = async () => {
  const queue = new Queue<{
    id: string;
    jobShouldTakeThisLongMs: number;
  }>({
    redis: redis,
    namespace: 'example',
    keepCompleted: 100,
    keepFailed: 100,
  });

  const cronQueue = new Queue<{
    id: string;
    jobShouldTakeThisLongMs: number;
  }>({
    redis: redis,
    namespace: 'cron',
    keepCompleted: 100,
    keepFailed: 100,
  });

  const worker = new Worker({
    queue: queue,
    async handler(job) {
      await sleep(job.data.jobShouldTakeThisLongMs);
      if (Math.random() > 0.9) {
        throw new Error('test error');
      }
      return `basic worker completed job ${job.id}`;
    },
  });

  const cronWorker = new Worker({
    queue: cronQueue,
    // Run cleanup/scheduler frequently so repeating jobs trigger on time
    cleanupIntervalMs: 1000,
    async handler(job) {
      await sleep(job.data.jobShouldTakeThisLongMs);
      if (Math.random() > 0.9) {
        throw new Error('test error');
      }
      return `cron worker completed job ${job.id}`;
    },
  });

  const workers = [worker, cronWorker];

  workers.forEach((worker) => {
    worker.on('completed', (job) => {
      console.log('job completed', job.id, {
        elapsed: job.finishedOn! - job.processedOn!,
      });
    });

    worker.on('error', (err) => {
      console.error('worker error', err);
    });

    worker.run();
  });

  const app = new Hono();

  const serverAdapter = new HonoAdapter(serveStatic);

  createBullBoard({
    queues: [
      new BullBoardGroupMQAdapter(queue),
      new BullBoardGroupMQAdapter(cronQueue),
    ],
    serverAdapter,
  });

  const basePath = '/ui';
  serverAdapter.setBasePath(basePath);
  app.route(basePath, serverAdapter.registerPlugin());

  // Add a cron job that runs every 5 seconds
  await cronQueue.removeRepeatingJob('groupId', { every: 5000 });
  await cronQueue.add({
    data: {
      id: Math.random().toString(36).substring(2, 15),
      jobShouldTakeThisLongMs: Math.random() * 1000,
    },
    groupId: 'groupId',
    repeat: { every: 5000 }, // every 5 seconds
  });

  // Add basic job every 2.5 seconds
  const groups = ['groupId', 'groupId2', 'groupId3', 'groupId4', 'groupId5'];
  setInterval(async () => {
    const groupId = groups[Math.floor(Math.random() * groups.length)];
    console.log('adding job regular job', groupId);

    await queue.add({
      data: {
        id: Math.random().toString(36).substring(2, 15),
        jobShouldTakeThisLongMs: Math.random() * 1000,
      },
      groupId,
    });
  }, 1_000);

  showRoutes(app);

  serve({ fetch: app.fetch, port: 3000 }, ({ address, port }) => {
    /* eslint-disable no-console */
    console.log(`Running on ${address}:${port}...`);
    console.log(`For the UI of instance1, open http://localhost:${port}/ui`);
    console.log('Make sure Redis is running on port 6379 by default');
    console.log('To populate the queue, run:');
    console.log(`  curl http://localhost:${port}/add?title=Example`);
    /* eslint-enable */
  });
};

run().catch((e) => {
  console.error(e);
  process.exit(1);
});

console.log('HEJ');
