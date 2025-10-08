
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

### Simplified API

- **No more polling vs blocking confusion**, always uses efficient blocking operations
- **Clear naming** - `jobTimeoutMs` instead of confusing `visibilityTimeoutMs`
- **Automatic namespace prefixing**, all namespaces get `groupmq:` prefix to avoid conflicts
- **Unified configuration**, no duplicate options between Queue and Worker
- **Built-in logging**, optional logger for debugging and monitoring (both Queue and Worker)

### Performance & Reliability

- **High throughput** - 70+ jobs/sec with 4 workers, 80+ jobs/sec at scale
- **1 in-flight job per group** via per-group locks
- **Parallel processing** across different groups with configurable concurrency
- **FIFO ordering** within each group by `orderMs` with stable tiebreaking
- **Multiple ordering strategies** - `'none'`, `'scheduler'`, or `'in-memory'` for handling out-of-order arrivals
- **At-least-once delivery** with configurable retries and exponential backoff
- **Efficient blocking operations** - no wasteful polling
- **Atomic operations** - all critical paths use Lua scripts for consistency
- **Stalled job detection** - automatic recovery when workers crash
- **Connection error handling** - exponential backoff and auto-reconnection
- **Graceful shutdown** - wait for jobs to complete before exiting

## Production-Ready Reliability

GroupMQ includes production-ready features for fault tolerance and resilience:

### Stalled Job Detection
Automatically detects and recovers jobs when workers crash or lose connection:
```ts
const worker = new Worker({
  queue,
  handler,
  stalledInterval: 30000,     // Check every 30 seconds
  maxStalledCount: 1,         // Fail after 1 stall
})

worker.on('stalled', (jobId, groupId) => {
  console.warn(`Job ${jobId} stalled and recovered`)
})
```

### Connection Error Handling
- Exponential backoff retry strategy (1s → 20s max)
- Automatic reconnection with infinite retries for blocking connections
- Smart error classification (connection vs application errors)

### Graceful Shutdown
```ts
process.on('SIGTERM', async () => {
  await worker.close(30000)  // Wait up to 30s for jobs to complete
  process.exit(0)
})
```

See the [Reliability Guide](https://openpanel-dev.github.io/groupmq/docs/reliability) for detailed configuration and best practices.

## Inspiration from BullMQ

GroupMQ is heavily inspired by [BullMQ](https://github.com/taskforcesh/bullmq), one of the most popular Redis-based job queue libraries for Node.js. We've taken many core concepts and design patterns from BullMQ while adapting them for our specific use case of per-group FIFO processing.

### Key differences from BullMQ:
- **Per-group FIFO ordering**, jobs within the same group are processed in strict order
- **Group-based concurrency**, only one job per group can be active at a time
- **Ordered processing**, built-in support for `orderMs` timestamp-based ordering
- **Cross-group parallelism**, multiple groups can be processed simultaneously
- **No job types**, simplified to a single job, instead use union typed data `{ type: 'paint', data: { ... } } | { type: 'repair', data: { ... } }` 

We're grateful to the BullMQ team for their excellent work and the foundation they've provided for the Redis job queue ecosystem.

### Queue Options

```ts
type QueueOptions = {
  redis: Redis;                    // Redis client instance (required)
  namespace: string;                // Unique queue name, gets 'groupmq:' prefix (required)
  logger?: boolean | LoggerInterface; // Enable logging (default: false)
  jobTimeoutMs?: number;            // Job processing timeout (default: 30000ms)
  maxAttempts?: number;             // Default max retry attempts (default: 3)
  reserveScanLimit?: number;        // Groups to scan when reserving (default: 20)
  keepCompleted?: number;           // Number of completed jobs to retain (default: 100)
  keepFailed?: number;              // Number of failed jobs to retain (default: 100)
  schedulerLockTtlMs?: number;      // Scheduler lock TTL (default: 1500ms)
  orderingMethod?: OrderingMethod;  // Ordering strategy (default: 'none')
  orderingWindowMs?: number;        // Time window for ordering (required for non-'none' methods)
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
  heartbeatMs?: number;                      // Heartbeat interval (default: jobTimeoutMs/3)
  onError?: (err: unknown, job?: ReservedJob<T>) => void; // Error handler
  maxAttempts?: number;                      // Max retry attempts (default: queue.maxAttempts)
  backoff?: BackoffStrategy;                 // Retry backoff function
  enableCleanup?: boolean;                   // Periodic cleanup (default: true)
  cleanupIntervalMs?: number;                // Cleanup frequency (default: 60000ms)
  schedulerIntervalMs?: number;              // Scheduler frequency (default: adaptive)
  blockingTimeoutSec?: number;               // Blocking reserve timeout (default: 5s)
  atomicCompletion?: boolean;                // Atomic completion + next reserve (default: true)
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

**Features:**
- View job counts by status (active, waiting, delayed, completed, failed)
- Browse and search jobs
- View job details, data, and error messages
- Retry failed jobs
- Remove jobs
- View queue metrics and statistics

**Installation:**
```bash
npm install @bull-board/api @bull-board/express
```

## How It Works

This section explains GroupMQ's internal architecture for contributors and those interested in understanding the implementation.

### High-Level Flow

```
1. Job Added → Enqueued to group's sorted set
2. Group Added → `:ready` set (if not locked)
3. Worker Reserves → Picks job from ready group, locks group
4. Worker Processes → Executes handler function
5. Job Completes → Unlocks group, re-adds to `:ready` if more jobs
6. Repeat → Next worker picks up next job from group
```

### Key Concepts

**Per-Group FIFO**: Jobs within the same `groupId` are processed in strict FIFO order based on `orderMs`. Only one job per group can be processed at a time across all workers.

**Cross-Group Parallelism**: Different groups can be processed simultaneously. A worker with `concurrency: 8` can process up to 8 jobs from different groups in parallel. Multiple workers can run independently for even higher throughput.

**Group Locking**: When a job is reserved, its group is locked (`:lock:{groupId}`) for the duration of the job timeout. This prevents other workers from picking up jobs from the same group.

**Ready Set**: The `:ready` sorted set contains all groups that have jobs waiting and are not currently locked. Workers scan this set to find work.

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
- **`:repeats`**, hash of repeating job definitions (groupId → config)

#### Job Lifecycle States

1. **Waiting**, job is in `:g:{groupId}` and group is in `:ready`
2. **Delayed**, job is in `:delayed` (scheduled for future)
3. **Active**, job is in `:processing` and group is locked
4. **Completed**, job is in `:completed` (retention)
5. **Failed**, job exceeded maxAttempts, moved to completed with failed status

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
score = (orderMs * 10000) + seq
```

- `orderMs`, user-provided timestamp for event ordering
- `seq`, auto-incrementing sequence for tiebreaking

This ensures:
- Jobs with earlier `orderMs` process first
- Jobs with same `orderMs` process in submission order
- Score is stable and sortable

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
5. **Trim Completed**: Remove old completed jobs per retention policy

### Performance Characteristics

**Benchmarks** (500 jobs, 4 workers, multi-process):
- **Throughput**: 68-73 jobs/sec (500 jobs), 80-86 jobs/sec (5000 jobs)
- **Latency**: P95 pickup ~5-5.5s, P95 processing ~45-50ms
- **Memory**: ~120-145 MB per worker process
- **CPU**: <1% average, <70% peak

**Optimizations:**
- **Batch Operations**: `reserveBatch` reduces round-trips for concurrent workers
- **Blocking Operations**: Efficient Redis BLPOP-style blocking prevents wasteful polling
- **Lua Scripts**: All critical paths are atomic, avoiding race conditions
- **Atomic Completion**: Complete job + reserve next in single operation
- **Minimal Data**: Jobs store only essential fields, keeps memory low
- **Score-Based Ordering**: O(log N) insertions and retrievals via sorted sets
- **Adaptive Behavior**: Scheduler intervals adjust based on ordering configuration

### Contributing

When modifying GroupMQ:

1. **Lua Scripts**: If changing Redis operations, update scripts in `src/lua/`
2. **Run Copy Script**: `node scripts/copy-lua.mjs` to update `dist/lua/`
3. **Add Tests**: All features must have passing tests
4. **Test Atomicity**: Consider race conditions and concurrent worker scenarios
5. **Benchmark**: Run `npm run benchmark` to verify performance isn't degraded

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
