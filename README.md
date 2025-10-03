
<p align="center">
  <img src="website/public/favicon/web-app-manifest-512x512.png" width="200px" height="200px" />
	<h1 align="center"><b>GroupMQ - Redis Group Queue</b></h1>
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
});

await queue.add({
  groupId: "user:42",
  data: { type: "charge", amount: 999 },
  orderMs: Date.now(), // or event.createdAtMs
  maxAttempts: 5,
});

const worker = new Worker({
  queue,
  handler: async (job) => {
    console.log(`Processing:`, job.data);
  },
});

worker.run();
```

## Key Features

### Simplified API

- **No more polling vs blocking confusion** - Always uses efficient blocking operations
- **Clear naming** - `jobTimeoutMs` instead of confusing `visibilityTimeoutMs`
- **Automatic namespace prefixing** - All namespaces get `groupmq:` prefix to avoid conflicts
- **Unified configuration** - No duplicate options between Queue and Worker

### Performance & Reliability

- **1 in-flight job per group** via per-group locks
- **Parallel processing** across different groups
- **FIFO ordering** within each group by `orderMs` with stable tiebreaking
- **At-least-once delivery** with configurable retries and backoff
- **Efficient blocking operations** - no wasteful polling

### Queue Options

```ts
type QueueOptions = {
  redis: Redis;
  namespace: string; // Required, gets 'groupmq:' prefix
  jobTimeoutMs?: number; // Job processing timeout (default: 30s)
  maxAttempts?: number; // Default max attempts (default: 3)
  reserveScanLimit?: number; // Ready groups scan limit (default: 20)
  orderingDelayMs?: number; // Delay for late events (default: 0)
};
```

### Worker Options

```ts
type WorkerOptions<T> = {
  queue: Queue; // Existing queue instance (shared config)
  name?: string; // Worker name for logging
  handler: (job: ReservedJob<T>) => Promise<void>;
  heartbeatMs?: number; // Heartbeat interval (default: jobTimeoutMs/3)
  onError?: (err: unknown, job?: ReservedJob<T>) => void;
  maxAttempts?: number; // Max retry attempts (default: 3)
  backoff?: BackoffStrategy; // Retry backoff function
  enableCleanup?: boolean; // Periodic cleanup (default: true)
  cleanupIntervalMs?: number; // Cleanup frequency (default: 60s)
  blockingTimeoutSec?: number; // Blocking timeout (default: 5s)
};
```

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
- Repeating jobs are enqueued during this cycle. To avoid drift, set `cleanupIntervalMs` to be less than or equal to your repeat cadence.
  - Example: for `repeat.every = 5000`, use `cleanupIntervalMs` ≈ 1000–2000 ms.
  - Very frequent repeats (≤ 1s) are possible but may increase Redis load; consider `cleanupIntervalMs` 200–500 ms.
- The scheduler is idempotent: it updates the next run time before enqueueing to prevent double runs.
- Each occurrence is a normal job with a fresh `jobId`, preserving per‑group FIFO semantics.
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

### Queue Status

```ts
// Get job counts by state
const counts = await queue.getJobCounts();
// { active: 5, waiting: 12, delayed: 3, total: 20, uniqueGroups: 8 }

// Get unique groups that have jobs
const groups = await queue.getUniqueGroups();
// ['user:123', 'user:456', 'order:789']

// Get count of unique groups
const groupCount = await queue.getUniqueGroupsCount();
// 8

// Get job IDs by state
const jobs = await queue.getJobs();
// { active: ['1', '2'], waiting: ['3', '4'], delayed: ['5'] }
```

### Worker Status

```ts
// Check if worker is processing a job
const isProcessing = worker.isProcessing();

// Get current job info (if any)
const currentJob = worker.getCurrentJob();
// { job: ReservedJob, processingTimeMs: 1500 } | null
```

## CLI Monitor

A built-in CLI tool for monitoring queue status in real-time:

```bash
# Install dependencies first
npm install

# Monitor a queue (basic usage)
npm run monitor -- --namespace orders

# Custom Redis URL and poll interval
npm run monitor -- --namespace orders --redis-url redis://localhost:6379 --interval 2000

# Show help
npm run monitor -- --help
```

The CLI displays:

- Real-time job counts (active, waiting, delayed, total)
- Number of unique groups
- List of active groups
- Updates every second (configurable)

Example output:

```
╔════════════════════════════════════════════════════════════════════╗
║                          GroupMQ Monitor                           ║
╚════════════════════════════════════════════════════════════════════╝

Namespace: orders
Poll Interval: 1000ms
Last Update: 2:30:45 PM

Job Counts:
  Active:           3
  Waiting:         12
  Delayed:          0
  Total:           15

Groups:
  Unique Groups:    8

Active Groups:
  ├─ user:123
  ├─ user:456
  ├─ order:789
  └─ payment:abc
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

**Per-Group FIFO**: Jobs within the same `groupId` are processed in strict FIFO order based on `orderMs`. Only one job per group can be processed at a time.

**Cross-Group Parallelism**: Different groups can be processed simultaneously by multiple workers. A worker with `concurrency: 8` can process 8 jobs from 8 different groups in parallel.

**Group Locking**: When a job is reserved, its group is locked (`:lock:{groupId}`) for the duration of the job timeout. This prevents other workers from picking up jobs from the same group.

**Ready Set**: The `:ready` sorted set contains all groups that have jobs waiting and are not currently locked. Workers scan this set to find work.

### Detailed Architecture

#### Redis Data Structures

GroupMQ uses these Redis keys (all prefixed with `groupmq:{namespace}:`):

- **`:g:{groupId}`** - Sorted set of job IDs in a group, ordered by score (derived from `orderMs` and `seq`)
- **`:ready`** - Sorted set of group IDs that have jobs available, ordered by lowest job score
- **`:job:{jobId}`** - Hash containing job data (id, groupId, data, attempts, status, etc.)
- **`:lock:{groupId}`** - String with job ID that currently owns the group lock (with TTL)
- **`:processing`** - Sorted set of active job IDs, ordered by deadline
- **`:processing:{jobId}`** - Hash with processing metadata (groupId, deadlineAt)
- **`:delayed`** - Sorted set of delayed jobs, ordered by runAt timestamp
- **`:completed`** - Sorted set of completed job IDs (for retention)
- **`:repeats`** - Hash of repeating job definitions (groupId → config)

#### Job Lifecycle States

1. **Waiting** - Job is in `:g:{groupId}` and group is in `:ready`
2. **Delayed** - Job is in `:delayed` (scheduled for future)
3. **Active** - Job is in `:processing` and group is locked
4. **Completed** - Job is in `:completed` (retention)
5. **Failed** - Job exceeded maxAttempts, moved to completed with failed status

#### Worker Loop

The worker runs a continuous loop with these steps:

```typescript
while (!stopping) {
  // 1. Run lightweight scheduler (every 100ms)
  await queue.runSchedulerOnce(); // Promotes delayed jobs, processes repeats
  
  // 2. Try batch reservation (if concurrency > 1)
  if (capacity > 0 && concurrency > 1) {
    const jobs = await queue.reserveBatch(capacity);
    // Process all jobs concurrently
    for (const job of jobs) {
      void processOne(job); // Fire and forget
    }
  }
  
  // 3. Blocking reserve (waits for job)
  const job = await queue.reserveBlocking(timeoutSec);
  
  // 4. Process job
  if (job) {
    if (concurrency > 1) {
      void processOne(job); // Process async
    } else {
      await processOne(job); // Process sync
    }
  }
}
```

#### Atomic Operations (Lua Scripts)

All critical operations use Lua scripts for atomicity:

- **`enqueue.lua`** - Adds job to group queue, adds group to ready set
- **`reserve.lua`** - Finds ready group, pops head job, locks group
- **`reserve-batch.lua`** - Reserves one job from multiple groups atomically
- **`complete.lua`** - Marks job complete, unlocks group, re-adds group to ready if more jobs
- **`complete-and-reserve-next.lua`** - Atomic completion + reservation from same group
- **`retry.lua`** - Increments attempts, re-adds job to group with backoff delay
- **`remove.lua`** - Removes job from all data structures

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

- `orderMs` - User-provided timestamp for event ordering
- `seq` - Auto-incrementing sequence for tiebreaking

This ensures:
- Jobs with earlier `orderMs` process first
- Jobs with same `orderMs` process in submission order
- Score is stable and sortable

#### Concurrency Modes

**concurrency = 1** (Sequential):
- Worker processes one job at a time
- Uses blocking reserve with synchronous processing
- Simplest mode, lowest memory

**concurrency > 1** (Parallel):
- Worker attempts batch reservation first (lower latency)
- Processes multiple jobs concurrently (different groups)
- Falls back to blocking reserve if batch is empty
- Higher throughput, can process multiple groups simultaneously

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

- **Batch Operations**: `reserveBatch` reduces round-trips for high-concurrency workers
- **Blocking Operations**: Efficient `BLPOP`-style blocking prevents polling
- **Lua Scripts**: All critical paths are atomic, avoiding race conditions
- **Minimal Data**: Jobs store only essential fields, keeps memory low
- **Score-Based Ordering**: O(log N) insertions and retrievals via sorted sets

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
