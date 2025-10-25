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
    groupId: string;
  }>({
    logger: true,
    redis: redis,
    namespace: 'example',
    keepCompleted: 100000,
    keepFailed: 100,
  });

  const CONCURRENCY = 4;
  const WORKERS = 4;

  const createWorker = (index: number, concurrency: number) => {
    return new Worker({
      concurrency: concurrency,
      queue: queue,
      async handler(job) {
        console.log(`processing job worker ${index}`, job.id);
        await sleep(job.data.jobShouldTakeThisLongMs);
      },
    });
  };

  const workers = Array.from({ length: WORKERS }, (_, i) =>
    createWorker(i, CONCURRENCY),
  );

  workers.forEach((worker) => {
    worker.on('completed', (job) => {
      const completedAt = Date.now();
      const addedAt = job.timestamp;
      const processedOn = job.processedOn;
      const finishedOn = job.finishedOn;

      console.log('\n📊 JOB TIMING BREAKDOWN:');
      console.log(
        `  ⏱️  Job added at:        ${new Date(addedAt).toISOString()}`,
      );
      console.log(
        `  ⏱️  Job processedOn:     ${new Date(processedOn!).toISOString()}`,
      );
      console.log(
        `  ⏱️  Job finishedOn:      ${new Date(finishedOn!).toISOString()}`,
      );
      console.log(
        `  ⏱️  Event fired at:      ${new Date(completedAt).toISOString()}`,
      );
      console.log(
        `  🔴 Add → ProcessedOn:    ${processedOn! - addedAt}ms ⚠️  THIS IS THE DELAY!`,
      );
      console.log(
        `  🟢 ProcessedOn → Finish: ${finishedOn! - processedOn!}ms (actual work)`,
      );
      console.log(`  🎯 Total:                ${finishedOn! - addedAt}ms\n`);
      console.log('✅ Job completed:', job.id);
    });

    worker.on('error', (err) => {
      console.error('worker error', err);
    });

    worker.run();
  });

  const app = new Hono();

  const serverAdapter = new HonoAdapter(serveStatic);

  createBullBoard({
    queues: [new BullBoardGroupMQAdapter(queue)],
    serverAdapter,
  });

  const basePath = '/ui';
  serverAdapter.setBasePath(basePath);
  app.route(basePath, serverAdapter.registerPlugin());

  // Add basic job every 2.5 seconds
  const groups = [
    ...new Array(25).fill(null).map((_, i) => `groupId_${i + 1}`),
  ];
  // setInterval(async () => {
  //   const groupId = groups[Math.floor(Math.random() * groups.length)]!;
  //   await queue.add({
  //     data: {
  //       id: Math.random().toString(36).substring(2, 15),
  //       jobShouldTakeThisLongMs: Math.random() * 1000,
  //       groupId,
  //     },
  //     groupId,
  //   });
  // }, 1_000);

  app.get('/add', async (c) => {
    const groups = [
      ...new Array(1000).fill(null).map((_, i) => `groupId_${i + 1}`),
    ];
    const events = [
      ...new Array(200).fill(null).map((_, i) => `event_${i + 1}`),
    ];
    for (const event of events) {
      const groupId = groups[Math.floor(Math.random() * groups.length)]!;
      await queue.add({
        data: {
          id: event,
          jobShouldTakeThisLongMs: Math.random() * 40 + 20,
          groupId,
        },
        groupId,
      });
    }

    return c.json({ message: `${events.length} jobs added` });
  });

  app.get('/add-single', async (c) => {
    const groupId = `groupId_${Math.floor(Math.random() * 100) + 1}`;
    const event = `event_${Math.floor(Math.random() * 200) + 1}`;
    await queue.add({
      data: {
        id: event,
        jobShouldTakeThisLongMs: Math.random() * 40 + 20,
        groupId,
      },
      groupId,
    });
    return c.json({ message: 'job added' });
  });

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
