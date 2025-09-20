# GroupMQ - Redis Group Queue

A fast, reliable Redis-backed per-group FIFO queue for Node + TypeScript with guaranteed job ordering and parallel processing across groups.

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
