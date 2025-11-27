
<p align="center">
  <img src="website/public/favicon/web-app-manifest-512x512.png" width="200px" height="200px" />
	<h1 align="center"><b>GroupMQ, Redis Group Queue</b></h1>
<p align="center">
    A fast, reliable Redis-backed per-group FIFO queue for Node + TypeScript with guaranteed job ordering and parallel processing across groups.
    <br />
    <br />
    <a href="https://openpanel-dev.github.io/groupmq/">Website</a>
    ·
    <a href="https://openpanel.dev">Created by OpenPanel.dev</a>
  </p>
  <br />
  <br />
</p>

## Install

```bash
npm i groupmq ioredis
```

## Quick start

```ts
import Redis from "ioredis";
import { Queue, Worker } from "groupmq";

const redis = new Redis("redis://127.0.0.1:6379");

const queue = new Queue({
  redis,
  namespace: "orders", // Will be prefixed with 'groupmq:'
  jobTimeoutMs: 30_000, // How long before job times out
  logger: true, // Enable logging (optional)
});

await queue.add({
  groupId: "user:42",
  data: { type: "charge", amount: 999 },
  orderMs: Date.now(), // or event.createdAtMs
  maxAttempts: 5,
});

const worker = new Worker({
  queue,
  concurrency: 1, // Process 1 job at a time (can increase for parallel processing)
  handler: async (job) => {
    console.log(`Processing:`, job.data);
  },
});

worker.run();
```

## Key Features


## Key Features

- **Per-group FIFO ordering** - Jobs within the same group process in strict order, perfect for user workflows, data pipelines, and sequential operations
- **Parallel processing across groups** - Process multiple groups simultaneously while maintaining order within each group
- **BullMQ-compatible API** - Familiar interface with enhanced group-based capabilities
- **High performance** - High throughput with low latency ([see benchmarks](https://openpanel-dev.github.io/groupmq/benchmarks/))
- **Built-in ordering strategies** - Handle out-of-order job arrivals with `'none'`, `'scheduler'`, or `'in-memory'` methods
- **Automatic recovery** - Stalled job detection and connection error handling with exponential backoff
- **Production ready** - Atomic operations, graceful shutdown, and comprehensive logging
- **Zero polling** - Efficient blocking operations prevent wasteful Redis calls

## Inspiration from BullMQ

GroupMQ is heavily inspired by [BullMQ](https://github.com/taskforcesh/bullmq), a fantastic library and one of the most popular Redis-based job queue libraries for Node.js. We've taken many core concepts and design patterns from BullMQ while adapting them for our specific use case of per-group FIFO processing.

### Key differences from BullMQ:
- **Per-group FIFO ordering**, jobs within the same group are processed in strict order
- **Group-based concurrency**, only one job per group can be active at a time
- **Ordered processing**, built-in support for `orderMs` timestamp-based ordering
- **Cross-group parallelism**, multiple groups can be processed simultaneously
- **No job types**, simplified to a single job, instead use union typed data `{ type: 'paint', data: { ... } } | { type: 'repair', data: { ... } }` 

We're grateful to the BullMQ team for their excellent work and the foundation they've provided for the Redis job queue ecosystem.

### Third-Party Code Attribution

While GroupMQ is inspired by BullMQ's design and concepts, we have also directly copied some code from BullMQ:

- **`src/async-fifo-queue.ts`** - This file contains code copied from BullMQ's AsyncFifoQueue implementation. BullMQ's implementation is well-designed and fits our needs perfectly, so we've used it directly rather than reimplementing it.

This code is used under the MIT License. The original copyright notice and license can be found at:
- BullMQ Repository: https://github.com/taskforcesh/bullmq
- BullMQ License: https://github.com/taskforcesh/bullmq/blob/main/LICENSE

Original copyright: Copyright (c) Taskforce.sh and contributors

### Queue Options

```ts
type QueueOptions = {
  redis: Redis;                    // Redis client instance (required)
  namespace: string;                // Unique queue name, gets 'groupmq:' prefix (required)
  logger?: boolean | LoggerInterface; // Enable logging (default: false)
  jobTimeoutMs?: number;            // Job processing timeout (default: 30000ms)
  maxAttempts?: number;             // Default max retry attempts (default: 3)
  reserveScanLimit?: number;        // Groups to scan when reserving (default: 20)
  keepCompleted?: number;           // Number of completed jobs to retain (default: 0)
  keepFailed?: number;              // Number of failed jobs to retain (default: 0)
  schedulerLockTtlMs?: number;      // Scheduler lock TTL (default: 1500ms)
  orderingMethod?: OrderingMethod;  // Ordering strategy (default: 'none')
  orderingWindowMs?: number;        // Time window for ordering (required for non-'none' methods)
  orderingMaxWaitMultiplier?: number; // Max grace period multiplier for in-memory (default: 3)
  orderingGracePeriodDecay?: number;  // Grace period decay factor for in-memory (default: 1.0)
  orderingMaxBatchSize?: number;      // Max jobs to collect in batch for in-memory (default: 10)
};

type OrderingMethod = 'none' | 'scheduler' | 'in-memory';
```

**Ordering Methods:**
- `'none'` - No ordering guarantees (fastest, zero overhead, no extra latency)
- `'scheduler'` - Redis buffering for large windows (≥1000ms, requires scheduler, adds latency)
- `'in-memory'` - Worker collection for small windows (50-500ms, no scheduler, adds latency per batch)

See [Ordering Methods](https://openpanel-dev.github.io/groupmq/docs/ordering-methods) for detailed comparison.

### Worker Options

```ts
type WorkerOptions<T> = {
  queue: Queue<T>;                           // Queue instance to process jobs from (required)
  handler: (job: ReservedJob<T>) => Promise<unknown>; // Job processing function (required)
  name?: string;                             // Worker name for logging (default: queue.name)
  logger?: boolean | LoggerInterface;        // Enable logging (default: false)
  concurrency?: number;                      // Number of jobs to process in parallel (default: 1)
  heartbeatMs?: number;                      // Heartbeat interval (default: Math.max(1000, jobTimeoutMs/3))
  onError?: (err: unknown, job?: ReservedJob<T>) => void; // Error handler
  maxAttempts?: number;                      // Max retry attempts (default: queue.maxAttempts)
  backoff?: BackoffStrategy;                 // Retry backoff function (default: exponential with jitter)
  enableCleanup?: boolean;                   // Periodic cleanup (default: true)
  cleanupIntervalMs?: number;                // Cleanup frequency (default: 60000ms)
  schedulerIntervalMs?: number;              // Scheduler frequency (default: adaptive)
  blockingTimeoutSec?: number;               // Blocking reserve timeout (default: 5s)
  atomicCompletion?: boolean;                // Atomic completion + next reserve (default: true)
  stalledInterval?: number;                  // Check if stalled every N ms (default: 30000)
  maxStalledCount?: number;                  // Fail after N stalls (default: 1)
  stalledGracePeriod?: number;               // Grace period before considering stalled (default: 0)
};

type BackoffStrategy = (attempt: number) => number; // returns delay in ms
```

### Job Options

When adding a job to the queue:

```ts
await queue.add({
  groupId: string;           // Required: Group ID for FIFO processing
  data: T;                   // Required: Job payload data
  orderMs?: number;          // Timestamp for ordering (default: Date.now())
  maxAttempts?: number;      // Max retry attempts (default: queue.maxAttempts)
  jobId?: string;            // Custom job ID (default: auto-generated UUID)
  delay?: number;            // Delay in ms before job becomes available
  runAt?: Date | number;     // Specific time to run the job
  repeat?: RepeatOptions;    // Repeating job configuration (cron or interval)
});

type RepeatOptions = 
  | { every: number }                    // Repeat every N milliseconds
  | { pattern: string };                 // Cron pattern (standard 5-field format)
```

**Example with delay:**
```ts
await queue.add({
  groupId: 'user:123',
  data: { action: 'send-reminder' },
  delay: 3600000, // Run in 1 hour
});
```

**Example with specific time:**
```ts
await queue.add({
  groupId: 'user:123',
  data: { action: 'scheduled-report' },
  runAt: new Date('2025-12-31T23:59:59Z'),
});
```

## Worker Concurrency

Workers support configurable concurrency to process multiple jobs in parallel from different groups:

```ts
const worker = new Worker({
  queue,
  concurrency: 8, // Process up to 8 jobs simultaneously
  handler: async (job) => {
    // Jobs from different groups can run in parallel
    // Jobs from the same group still run sequentially
  },
});
```

**Benefits:**
- Higher throughput for multi-group workloads
- Efficient resource utilization
- Still maintains per-group FIFO ordering

**Considerations:**
- Each job consumes memory and resources
- Set concurrency based on job duration and system resources
- Monitor Redis connection pool (ioredis default: 10 connections)

## Logging

Both Queue and Worker support optional logging for debugging and monitoring:

```ts
// Enable default logger
const queue = new Queue({
  redis,
  namespace: 'orders',
  logger: true, // Logs to console with queue name prefix
});

const worker = new Worker({
  queue,
  logger: true, // Logs to console with worker name prefix
  handler: async (job) => { /* ... */ },
});
```

**Custom logger:**

Works out of the box with both `pino` and `winston`

```ts
import type { LoggerInterface } from 'groupmq';

const customLogger: LoggerInterface = {
  debug: (msg: string, ...args: any[]) => { /* custom logging */ },
  info: (msg: string, ...args: any[]) => { /* custom logging */ },
  warn: (msg: string, ...args: any[]) => { /* custom logging */ },
  error: (msg: string, ...args: any[]) => { /* custom logging */ },
};

const queue = new Queue({
  redis,
  namespace: 'orders',
  logger: customLogger,
});
```

**What gets logged:**
- Job reservation and completion
- Error handling and retries
- Scheduler runs and delayed job promotions
- Group locking and unlocking
- Redis connection events
- Performance warnings

## Repeatable jobs (cron/interval)

GroupMQ supports simple repeatable jobs using either a fixed interval (`every`) or a basic cron pattern (`pattern`). Repeats are materialized by a lightweight scheduler that runs as part of the worker's periodic cleanup cycle.

### Add a repeating job (every N ms)

```ts
await queue.add({
  groupId: 'reports',
  data: { type: 'daily-summary' },
  repeat: { every: 5000 }, // run every 5 seconds
});

const worker = new Worker({
  queue,
  handler: async (job) => {
    // process...
  },
  // IMPORTANT: For timely repeats, run the scheduler frequently
  cleanupIntervalMs: 1000, // <= repeat.every (recommended 1–2s for 5s repeats)
});

worker.run();
```

### Add a repeating job (cron pattern)

```ts
await queue.add({
  groupId: 'emails',
  data: { type: 'weekly-digest' },
  repeat: { pattern: '0 9 * * 1-5' }, // 09:00 Mon–Fri
});
```

### Remove a repeating job

```ts
await queue.removeRepeatingJob('reports', { every: 5000 });
// or
await queue.removeRepeatingJob('emails', { pattern: '0 9 * * 1-5' });
```

### Scheduler behavior and best practices

- The worker's periodic cycle runs: `cleanup()`, `promoteDelayedJobs()`, and `processRepeatingJobs()`.
- Repeating jobs are enqueued during this cycle via a distributed scheduler with lock coordination.
- **Minimum practical repeat interval:** ~1.5-2 seconds (controlled by `schedulerLockTtlMs`, default: 1500ms)
- For sub-second repeats (not recommended in production):
  ```ts
  const queue = new Queue({
    redis,
    namespace: 'fast',
    schedulerLockTtlMs: 50, // Allow fast scheduler lock
  });
  
  const worker = new Worker({
    queue,
    schedulerIntervalMs: 10,   // Check every 10ms
    cleanupIntervalMs: 100,    // Cleanup every 100ms
    handler: async (job) => { /* ... */ },
  });
  ```
  ⚠️ Fast repeats (< 1s) increase Redis load and should be used sparingly.
- The scheduler is idempotent: it updates the next run time before enqueueing to prevent double runs.
- Each occurrence is a normal job with a fresh `jobId`, preserving per-group FIFO semantics.
- You can monitor repeated runs via BullBoard using the provided adapter.

## Graceful Shutdown

```ts
// Stop worker gracefully - waits for current job to finish
await worker.close(gracefulTimeoutMs);

// Wait for queue to be empty
const isEmpty = await queue.waitForEmpty(timeoutMs);

// Recover groups that might be stuck due to ordering delays
const recoveredCount = await queue.recoverDelayedGroups();
```

## Additional Methods

### Queue Methods

```ts
// Job counts and status
const counts = await queue.getJobCounts();
// { active: 5, waiting: 12, delayed: 3, total: 20, uniqueGroups: 8 }

const activeCount = await queue.getActiveCount();
const waitingCount = await queue.getWaitingCount();
const delayedCount = await queue.getDelayedCount();
const completedCount = await queue.getCompletedCount();
const failedCount = await queue.getFailedCount();

// Get job IDs by status
const activeJobIds = await queue.getActiveJobs();
const waitingJobIds = await queue.getWaitingJobs();
const delayedJobIds = await queue.getDelayedJobs();

// Get Job instances by status
const completedJobs = await queue.getCompletedJobs(limit); // returns Job[]
const failedJobs = await queue.getFailedJobs(limit);

// Group information
const groups = await queue.getUniqueGroups(); // ['user:123', 'order:456']
const groupCount = await queue.getUniqueGroupsCount();
const jobsInGroup = await queue.getGroupJobCount('user:123');

// Get specific job
const job = await queue.getJob(jobId); // returns Job instance

// Job manipulation
await queue.remove(jobId);
await queue.retry(jobId); // Re-enqueue a failed job
await queue.promote(jobId); // Promote delayed job to waiting
await queue.changeDelay(jobId, newDelayMs);
await queue.updateData(jobId, newData);

// Scheduler operations
await queue.runSchedulerOnce(); // Manual scheduler run
await queue.promoteDelayedJobs(); // Promote delayed jobs
await queue.recoverDelayedGroups(); // Recover stuck groups

// Cleanup and shutdown
await queue.waitForEmpty(timeoutMs);
await queue.close();
```

### Job Instance Methods

Jobs returned from `queue.getJob()`, `queue.getCompletedJobs()`, etc. have these methods:

```ts
const job = await queue.getJob(jobId);

// Manipulate the job
await job.remove();
await job.retry();
await job.promote();
await job.changeDelay(newDelayMs);
await job.updateData(newData);
await job.update(newData); // Alias for updateData

// Get job state
const state = await job.getState(); // 'active' | 'waiting' | 'delayed' | 'completed' | 'failed'

// Serialize job
const json = job.toJSON();
```

### Worker Methods

```ts
// Check worker status
const isProcessing = worker.isProcessing();

// Get current job(s) being processed
const currentJob = worker.getCurrentJob();
// { job: ReservedJob, processingTimeMs: 1500 } | null

// For concurrency > 1
const currentJobs = worker.getCurrentJobs();
// [{ job: ReservedJob, processingTimeMs: 1500 }, ...]

// Get worker metrics
const metrics = worker.getWorkerMetrics();
// { jobsInProgress: 2, lastJobPickupTime: 1234567890, ... }

// Graceful shutdown
await worker.close(gracefulTimeoutMs);
```

### Worker Events

Workers emit events that you can listen to:

```ts
worker.on('ready', () => {
  console.log('Worker is ready');
});

worker.on('completed', (job: Job) => {
  console.log('Job completed:', job.id);
});

worker.on('failed', (job: Job) => {
  console.log('Job failed:', job.id, job.failedReason);
});

worker.on('error', (error: Error) => {
  console.error('Worker error:', error);
});

worker.on('closed', () => {
  console.log('Worker closed');
});

worker.on('graceful-timeout', (job: Job) => {
  console.log('Job exceeded graceful timeout:', job.id);
});

// Remove event listeners
worker.off('completed', handler);
worker.removeAllListeners();
```

### BullBoard Integration

GroupMQ provides a BullBoard adapter for visual monitoring and management:

```ts
import { createBullBoard } from '@bull-board/api';
import { ExpressAdapter } from '@bull-board/express';
import { BullBoardGroupMQAdapter } from 'groupmq';
import express from 'express';

const serverAdapter = new ExpressAdapter();
serverAdapter.setBasePath('/admin/queues');

createBullBoard({
  queues: [
    new BullBoardGroupMQAdapter(queue, {
      displayName: 'Order Processing',
      description: 'Processes customer orders',
      readOnlyMode: false, // Allow job manipulation through UI
    }),
  ],
  serverAdapter,
});

const app = express();
app.use('/admin/queues', serverAdapter.getRouter());
app.listen(3000, () => {
  console.log('BullBoard running at http://localhost:3000/admin/queues');
});
```

### Detailed Architecture

#### Redis Data Structures

GroupMQ uses these Redis keys (all prefixed with `groupmq:{namespace}:`):

- **`:g:{groupId}`**, sorted set of job IDs in a group, ordered by score (derived from `orderMs` and `seq`)
- **`:ready`**, sorted set of group IDs that have jobs available, ordered by lowest job score
- **`:job:{jobId}`**, hash containing job data (id, groupId, data, attempts, status, etc.)
- **`:lock:{groupId}`**, string with job ID that currently owns the group lock (with TTL)
- **`:processing`**, sorted set of active job IDs, ordered by deadline
- **`:processing:{jobId}`**, hash with processing metadata (groupId, deadlineAt)
- **`:delayed`**, sorted set of delayed jobs, ordered by runAt timestamp
- **`:completed`**, sorted set of completed job IDs (for retention)
- **`:failed`**, sorted set of failed job IDs (for retention)
- **`:repeats`**, hash of repeating job definitions (groupId → config)

#### Job Lifecycle States

1. **Waiting**, job is in `:g:{groupId}` and group is in `:ready`
2. **Delayed**, job is in `:delayed` (scheduled for future)
3. **Active**, job is in `:processing` and group is locked
4. **Completed**, job is in `:completed` (retention)
5. **Failed**, job exceeded maxAttempts, moved to `:failed` (retention)

#### Worker Loop

The worker runs a continuous loop optimized for both single and concurrent processing:

**For concurrency = 1 (sequential):**
```typescript
while (!stopping) {
  // 1. Blocking reserve (waits for job, efficient)
  const job = await queue.reserveBlocking(timeoutSec);
  
  // 2. Process job synchronously
  if (job) {
    await processOne(job);
  }
  
  // 3. Periodic scheduler run (every schedulerIntervalMs)
  await queue.runSchedulerOnce(); // Promotes delayed jobs, processes repeats
}
```

**For concurrency > 1 (parallel):**
```typescript
while (!stopping) {
  // 1. Run lightweight scheduler periodically
  await queue.runSchedulerOnce();
  
  // 2. Try batch reservation if we have capacity
  const capacity = concurrency - jobsInProgress.size;
  if (capacity > 0) {
    const jobs = await queue.reserveBatch(capacity);
    // Process all jobs concurrently (fire and forget)
    for (const job of jobs) {
      void processOne(job);
    }
  }
  
  // 3. Blocking reserve for remaining capacity
  const job = await queue.reserveBlocking(blockingTimeoutSec);
  if (job) {
    void processOne(job); // Process async
  }
}
```

**Key optimizations:**
- Batch reservation reduces Redis round-trips for concurrent workers
- Blocking operations prevent wasteful polling
- Heartbeat mechanism keeps jobs alive during long processing
- Atomic completion + next reservation reduces latency

#### Atomic Operations (Lua Scripts)

All critical operations use Lua scripts for atomicity:

- **`enqueue.lua`**, adds job to group queue, adds group to ready set
- **`reserve.lua`**, finds ready group, pops head job, locks group
- **`reserve-batch.lua`**, reserves one job from multiple groups atomically
- **`complete.lua`**, marks job complete, unlocks group, re-adds group to ready if more jobs
- **`complete-and-reserve-next.lua`**, atomic completion + reservation from same group
- **`retry.lua`**, increments attempts, re-adds job to group with backoff delay
- **`remove.lua`**, removes job from all data structures

#### Job Reservation Flow

When a worker reserves a job:

1. **Find Ready Group**: `ZRANGE :ready 0 0` gets lowest-score group
2. **Check Lock**: `PTTL :lock:{groupId}` ensures group isn't locked
3. **Pop Job**: `ZPOPMIN :g:{groupId} 1` gets head job atomically
4. **Lock Group**: `SET :lock:{groupId} {jobId} PX {timeout}`
5. **Mark Processing**: Add to `:processing` sorted set with deadline
6. **Re-add Group**: If more jobs exist, `ZADD :ready {score} {groupId}`

#### Job Completion Flow

When a job completes successfully:

1. **Remove from Processing**: `DEL :processing:{jobId}`, `ZREM :processing {jobId}`
2. **Mark Completed**: `HSET :job:{jobId} status completed`
3. **Add to Retention**: `ZADD :completed {now} {jobId}`
4. **Unlock Group**: `DEL :lock:{groupId}` (only if this job owns the lock)
5. **Check for More Jobs**: `ZCARD :g:{groupId}`
6. **Re-add to Ready**: If jobs remain, `ZADD :ready {nextScore} {groupId}`

The critical fix in step 6 ensures that after a job completes, the group becomes available again for other workers to pick up the next job in the queue.

#### Ordering and Scoring

Jobs are ordered using a composite score:

```typescript
score = (orderMs - baseEpoch) * 1000 + seq
```

- `orderMs`, user-provided timestamp for event ordering
- `baseEpoch`, fixed epoch timestamp (1704067200000) to keep scores manageable
- `seq`, auto-incrementing sequence for tiebreaking (resets daily to prevent overflow)

This ensures:
- Jobs with earlier `orderMs` process first
- Jobs with same `orderMs` process in submission order
- Score is stable and sortable
- Daily sequence reset prevents integer overflow

#### Concurrency Modes

**concurrency = 1** (Sequential):
- Worker processes one job at a time
- Uses blocking reserve with synchronous processing
- Simplest mode, lowest memory, lowest Redis overhead
- Best for: CPU-intensive jobs, resource-constrained environments

**concurrency > 1** (Parallel):
- Worker attempts batch reservation first (lower latency)
- Processes multiple jobs concurrently (from different groups only)
- Each job runs in parallel with its own heartbeat
- Falls back to blocking reserve when batch is empty
- Higher throughput, efficient for I/O-bound workloads
- Best for: Network calls, database operations, API requests

**Important:** Per-group FIFO ordering is maintained regardless of concurrency level. Multiple jobs from the same group never run in parallel.

#### Error Handling and Retries

When a job fails:

1. **Increment Attempts**: `HINCRBY :job:{jobId} attempts 1`
2. **Check Max Attempts**: If `attempts >= maxAttempts`, mark as failed
3. **Calculate Backoff**: Use exponential backoff strategy
4. **Re-enqueue**: Add job back to `:g:{groupId}` with delay
5. **Unlock Group**: Release lock so next job can process

If a job times out (visibility timeout expires):
- Heartbeat mechanism extends the lock: `SET :lock:{groupId} {jobId} PX {timeout}`
- If heartbeat fails, job remains locked until TTL expires
- Cleanup cycle detects expired locks and recovers jobs

#### Cleanup and Recovery

Periodic cleanup runs:

1. **Promote Delayed Jobs**: Move jobs from `:delayed` to waiting when `runAt` arrives
2. **Process Repeats**: Enqueue next occurrence of repeating jobs
3. **Recover Stale Locks**: Find expired locks in `:processing` and unlock groups
4. **Recover Delayed Groups**: Handle groups stuck due to ordering delays
5. **Trim Completed/Failed**: Remove old completed and failed jobs per retention policy

### Performance Characteristics

**Latest Benchmarks** (MacBook M2, 500 jobs, 4 workers, multi-process):

#### GroupMQ Performance
- **Throughput**: 68-73 jobs/sec (500 jobs), 80-86 jobs/sec (5000 jobs)
- **Latency**: P95 pickup ~5-5.5s, P95 processing ~45-50ms
- **Memory**: ~120-145 MB per worker process
- **CPU**: <1% average, <70% peak

#### vs BullMQ Comparison
GroupMQ maintains competitive performance while adding per-group FIFO ordering guarantees:
- **Similar throughput** for group-based workloads
- **Better job ordering** with guaranteed per-group FIFO processing
- **Atomic operations** reduce race conditions and improve reliability

For detailed benchmark results and comparisons over time, see our [Performance Benchmarks](https://openpanel-dev.github.io/groupmq/benchmarks/) page.

**Optimizations:**
- **Batch Operations**: `reserveBatch` reduces round-trips for concurrent workers
- **Blocking Operations**: Efficient Redis BLPOP-style blocking prevents wasteful polling
- **Lua Scripts**: All critical paths are atomic, avoiding race conditions
- **Atomic Completion**: Complete job + reserve next in single operation
- **Minimal Data**: Jobs store only essential fields, keeps memory low
- **Score-Based Ordering**: O(log N) insertions and retrievals via sorted sets
- **Adaptive Behavior**: Scheduler intervals adjust based on ordering configuration

### Contributing

Contributions are welcome! When making changes:

1. **Run tests and benchmarks** before and after your changes to verify everything works correctly
2. **Add tests** for any new features

## Testing

Requires a local Redis at `127.0.0.1:6379` (no auth).

```bash
npm i
npm run build
npm test
```

Optionally:

```bash
docker run --rm -p 6379:6379 redis:7
```
