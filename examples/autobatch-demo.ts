import Redis from 'ioredis';
import { Queue, Worker } from '../src';

/**
 * Auto-Batch Demo
 *
 * This example demonstrates the auto-batching feature which can dramatically
 * reduce Redis load for high-throughput scenarios.
 *
 * WITHOUT autoBatch:
 * - 1000 jobs/s = 1000 Redis calls/s
 * - Each add() makes an immediate Redis call
 *
 * WITH autoBatch:
 * - 1000 jobs/s = ~100 Redis calls/s (10x reduction!)
 * - Jobs are buffered and sent in batches of 10
 * - Maximum 10ms delay before batch is flushed
 *
 * Trade-offs:
 * ✅ 10x fewer Redis calls
 * ✅ Higher throughput
 * ✅ Lower latency per add() call
 * ⚠️  Jobs buffered in memory briefly (10ms default)
 * ⚠️  If process crashes during batch window, those jobs are lost
 */

async function runDemo() {
  console.log('🚀 Auto-Batch Demo\n');

  // Clean up
  const redisCleanup = new Redis({ maxRetriesPerRequest: null });
  const keys = await redisCleanup.keys('groupmq:autobatch-demo:*');
  if (keys.length > 0) {
    await redisCleanup.del(...keys);
  }
  await redisCleanup.quit();

  // ============ WITHOUT Auto-Batch ============
  console.log('📊 Testing WITHOUT autoBatch...');
  const redis1 = new Redis({ maxRetriesPerRequest: null });
  const queueNoBatch = new Queue({
    redis: redis1,
    namespace: 'autobatch-demo',
    // No autoBatch
  });

  const startNoBatch = Date.now();
  const jobsNoBatch = await Promise.all(
    Array.from({ length: 100 }, (_, i) =>
      queueNoBatch.add({
        groupId: `group-${i % 10}`,
        data: { index: i },
      }),
    ),
  );
  const durationNoBatch = Date.now() - startNoBatch;

  console.log(
    `✅ Added ${jobsNoBatch.length} jobs in ${durationNoBatch}ms (${(jobsNoBatch.length / (durationNoBatch / 1000)).toFixed(0)} jobs/s)`,
  );
  console.log(`   Each job required ~1 Redis call\n`);

  await queueNoBatch.close();

  // Clean up for next test
  const redisCleanup2 = new Redis({ maxRetriesPerRequest: null });
  const keys2 = await redisCleanup2.keys('groupmq:autobatch-demo:*');
  if (keys2.length > 0) {
    await redisCleanup2.del(...keys2);
  }
  await redisCleanup2.quit();

  // ============ WITH Auto-Batch ============
  console.log('📊 Testing WITH autoBatch...');
  const redis2 = new Redis({ maxRetriesPerRequest: null });
  const queueWithBatch = new Queue({
    redis: redis2,
    namespace: 'autobatch-demo',
    autoBatch: true, // Enable with defaults
  });

  const startWithBatch = Date.now();
  const jobsWithBatch = await Promise.all(
    Array.from({ length: 100 }, (_, i) =>
      queueWithBatch.add({
        groupId: `group-${i % 10}`,
        data: { index: i },
      }),
    ),
  );
  const durationWithBatch = Date.now() - startWithBatch;

  console.log(
    `✅ Added ${jobsWithBatch.length} jobs in ${durationWithBatch}ms (${(jobsWithBatch.length / (durationWithBatch / 1000)).toFixed(0)} jobs/s)`,
  );
  console.log(
    `   Jobs batched into ~${Math.ceil(jobsWithBatch.length / 10)} Redis calls\n`,
  );

  // ============ Speed Comparison ============
  const speedup = durationNoBatch / durationWithBatch;
  console.log('📈 Performance Comparison:');
  console.log(`   Without autoBatch: ${durationNoBatch}ms`);
  console.log(`   With autoBatch:    ${durationWithBatch}ms`);
  console.log(`   Speedup:           ${speedup.toFixed(2)}x faster! 🎉\n`);

  // ============ Process the jobs ============
  console.log('👷 Processing jobs...');
  const processed: string[] = [];

  const worker = new Worker({
    queue: queueWithBatch,
    handler: async (job) => {
      processed.push(job.id);
      // Simulate some work
      await new Promise((resolve) => setTimeout(resolve, 10));
    },
    concurrency: 5,
  });

  // Wait for all jobs to be processed
  while (processed.length < jobsWithBatch.length) {
    await new Promise((resolve) => setTimeout(resolve, 100));
  }

  console.log(`✅ Processed ${processed.length} jobs\n`);

  // ============ Custom Configuration ============
  console.log('⚙️  Custom Configuration Example:');
  console.log(`
  const queue = new Queue({
    redis,
    namespace: 'my-queue',
    autoBatch: {
      size: 20,        // Batch size (default: 10)
      maxWaitMs: 5,    // Max wait time (default: 10ms)
    },
  });
  `);

  console.log('✨ Best Practices:');
  console.log('   - Use for high-volume scenarios (>100 jobs/s)');
  console.log('   - Keep maxWaitMs small (5-10ms) to minimize data loss risk');
  console.log('   - Implement graceful shutdown to flush pending batches');
  console.log(
    '   - Not recommended for critical jobs that must persist immediately\n',
  );

  // Cleanup
  await worker.close();
  await queueWithBatch.close();

  const redisCleanup3 = new Redis({ maxRetriesPerRequest: null });
  const keys3 = await redisCleanup3.keys('groupmq:autobatch-demo:*');
  if (keys3.length > 0) {
    await redisCleanup3.del(...keys3);
  }
  await redisCleanup3.quit();

  console.log('✅ Demo complete!');
}

runDemo().catch(console.error);
