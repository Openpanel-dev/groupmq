import Redis from 'ioredis';
import { Queue } from '../src/index';

async function debugTest() {
  const redis = new Redis('redis://127.0.0.1:6379');
  const namespace = `debug:${Date.now()}`;

  try {
    console.log('=== DEBUG: Testing orderingDelayMs ===');

    const queue = new Queue({
      redis,
      namespace,
      orderingDelayMs: 100,
    });

    const now = Date.now();
    console.log('Current time:', now);

    // Add first job
    console.log('\n1. Adding job1 (order: 1, timestamp: now)');
    const job1 = await queue.add({
      groupId: 'group1',
      data: { order: 1 },
      orderMs: now,
    });
    console.log('Job1 added:', job1.id);

    // Check Redis directly
    const keys = await redis.keys(`${namespace}*`);
    console.log('Redis keys after job1:', keys);

    // Try to reserve
    console.log('\n2. Trying to reserve after job1...');
    const jobs1 = await queue.reserveBatch(10);
    console.log('Reserved jobs after job1:', jobs1.length);

    // Add second job
    console.log('\n3. Adding job2 (order: 2, timestamp: now + 100)');
    const job2 = await queue.add({
      groupId: 'group1',
      data: { order: 2 },
      orderMs: now + 100,
    });
    console.log('Job2 added:', job2.id);

    // Check Redis again
    const keys2 = await redis.keys(`${namespace}*`);
    console.log('Redis keys after job2:', keys2);

    // Wait for ordering delay
    console.log('\n4. Waiting 150ms for orderingDelayMs...');
    await new Promise((resolve) => setTimeout(resolve, 150));

    // Try to reserve again
    console.log('\n5. Trying to reserve after waiting...');
    const jobs2 = await queue.reserveBatch(10);
    console.log('Reserved jobs after waiting:', jobs2.length);

    if (jobs2.length > 0) {
      jobs2.forEach((job, i) => {
        console.log(
          `  Job ${i + 1}: order=${job.data.order}, timestamp=${job.orderMs}`,
        );
      });
    }

    await queue.close();
  } finally {
    await redis.quit();
  }
}

debugTest().catch(console.error);
