## Suggested scenarios to run

Baseline (CPU-bound)
```
jiti benchmark/index.ts --mq bullmq --job cpu --concurrency 8 --jobs 10000 --name bullmq_cpu_c8
jiti benchmark/index.ts --mq groupmq --job cpu --concurrency 8 --jobs 10000 --name groupmq_cpu_c8
```


## IO-bound (simulate high IOPS)

```
jiti benchmark/index.ts --mq bullmq --job io --concurrency 32 --duration 60 --rate 0 --name bullmq_io_c32
jiti benchmark/index.ts --mq groupmq --job io --concurrency 32 --duration 60 --rate 0 --name groupmq_io_c32
```

## Backpressure & pickup latency under bursty load

```
jiti benchmark/index.ts --mq bullmq --job cpu --concurrency 4 --rate 200 --duration 30 --name bullmq_burst
jiti benchmark/index.ts --mq groupmq --job cpu --concurrency 4 --rate 200 --duration 30 --name groupmq_burst
```

## Failure & retry behavior

```
jiti benchmark/index.ts --mq bullmq --job io --concurrency 16 --jobs 5000 --fail-rate 0.05 --retries 3 --name bullmq_fail_retry
jiti benchmark/index.ts --mq groupmq --job io --concurrency 16 --jobs 5000 --fail-rate 0.05 --retries 3 --name groupmq_fail_retry
```