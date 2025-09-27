'use client';

import { CheckIcon, ClockIcon, CopyIcon, PlayIcon } from 'lucide-react';
import { AnimatePresence, motion } from 'motion/react';
import * as React from 'react';
import { FaGithub } from 'react-icons/fa6';
import { Code } from '@/components/code';
import { Button } from '@/components/ui/button';
import { Card, CardContent } from '@/components/ui/card';

const QUEUE_SETUP = `import Redis from "ioredis";
import { Queue, Worker } from "groupmq";

const redis = new Redis("redis://127.0.0.1:6379");

export const queue = new Queue<{ type: string; amount: number }>({
  redis,
  namespace: "orders",
});

await queue.add({
  groupId: "user:42",
  data: { type: "charge", amount: 999 },
});

const worker = new Worker({
  queue,
  handler: async (job) => {
    console.log("Processing:", job.data.type, job.data.amount);
  },
});

worker.run();
`;

export function GroupMQHero() {
  const [copied, setCopied] = React.useState(false);
  const resetTimer = React.useRef<number | null>(null);

  React.useEffect(() => {
    return () => {
      if (resetTimer.current) {
        window.clearTimeout(resetTimer.current);
        resetTimer.current = null;
      }
    };
  }, []);
  return (
    <section className="container">
      <div className="bordered-div-padding border-x border-b">
        <div className="flex flex-col gap-6 md:gap-8 lg:gap-10">
          <div className="space-y-4">
            <h1 className="font-weight-display text-2xl leading-snug tracking-tighter md:text-3xl lg:text-4xl">
              GroupMQ - per-group FIFO Queue for Node + Redis
            </h1>
            <p className="text-muted-foreground max-w-[820px] text-sm leading-relaxed md:text-lg lg:text-xl">
              Guaranteed ordering within a group, parallel processing across
              groups. Simple API, fast Lua scripts, and compatible with
              BullBoard.
            </p>
            <div className="flex gap-3">
              <Button asChild>
                <a
                  href="https://github.com/openpanel-dev/groupmq"
                  target="_blank"
                  rel="noreferrer"
                >
                  <FaGithub className="size-4" />
                  GitHub
                </a>
              </Button>
              <Button
                onClick={() => {
                  navigator.clipboard.writeText('npm i groupmq ioredis');
                  setCopied(true);
                  if (resetTimer.current) {
                    window.clearTimeout(resetTimer.current);
                  }
                  resetTimer.current = window.setTimeout(() => {
                    setCopied(false);
                  }, 1500);
                }}
                variant="outline"
                className="font-mono text-muted-foreground"
              >
                npm i <span className="text-foreground">groupmq</span> ioredis
                <span className="ml-8 relative inline-flex items-center justify-center">
                  <AnimatePresence initial={false} mode="wait">
                    {copied ? (
                      <motion.span
                        key="check"
                        initial={{ scale: 0.6, opacity: 0, rotate: -15 }}
                        animate={{ scale: 1, opacity: 1, rotate: 0 }}
                        exit={{ scale: 0.6, opacity: 0, rotate: 15 }}
                        transition={{
                          type: 'spring',
                          stiffness: 500,
                          damping: 30,
                        }}
                        className="text-green-500"
                      >
                        <CheckIcon className="size-4" />
                      </motion.span>
                    ) : (
                      <motion.span
                        key="copy"
                        initial={{ scale: 1, opacity: 1 }}
                        animate={{ scale: [1, 1.15, 1], opacity: 1 }}
                        transition={{ duration: 0.5, repeat: 0 }}
                      >
                        <CopyIcon className="size-4" />
                      </motion.span>
                    )}
                  </AnimatePresence>
                  {copied && (
                    <AnimatePresence>
                      <motion.span
                        key="burst"
                        className="absolute"
                        initial={{ scale: 0.6, opacity: 0 }}
                        animate={{ scale: 1.6, opacity: 0.15 }}
                        exit={{ opacity: 0 }}
                        transition={{ duration: 0.35 }}
                        style={{
                          width: 16,
                          height: 16,
                          borderRadius: 9999,
                          border: '2px solid #22c55e',
                        }}
                      />
                    </AnimatePresence>
                  )}
                </span>
              </Button>
            </div>
          </div>

          <div className="grid grid-cols-1 gap-6 lg:grid-cols-2">
            <div className="flex flex-col gap-6">
              <Card>
                <CardContent>
                  <Code>{QUEUE_SETUP}</Code>
                </CardContent>
              </Card>
            </div>

            <AnimatedWorker />
          </div>
        </div>
      </div>
    </section>
  );
}

type Job = {
  id: number;
  label: string;
  stage: 0 | 1 | 2; // 0 waiting, 1 active, 2 done
  color: string;
  waitMs: number;
  processMs: number;
  stageStartedAt: number;
};

function AnimatedWorker() {
  const colors = ['#22c55e', '#0ea5e9', '#f59e0b', '#dd2c5a'];
  const initialJobs = React.useMemo<Job[]>(
    () =>
      Array.from({ length: 3 }, (_, i) => ({
        id: i,
        label: `Job ${i + 1}`,
        stage: 0,
        color: colors[i],
        waitMs: Math.floor(Math.random() * 600) + 600,
        processMs: Math.floor(Math.random() * 800) + 800,
        stageStartedAt: Date.now(),
      })),
    [],
  );
  const [jobs, setJobs] = React.useState<Job[]>(initialJobs);
  const nextId = React.useRef(initialJobs.length);
  const [now, setNow] = React.useState(() => Date.now());

  React.useEffect(() => {
    const spawn = setInterval(() => {
      setJobs((prev) => {
        const id = nextId.current++;
        const color = colors[id % colors.length]!;
        const waitMs = 600 + Math.random() * 1200; // waiting -> active
        const processMs = 700 + Math.random() * 1400; // active -> completed
        const job: Job = {
          id,
          label: `Job ${id + 1}`,
          stage: 0,
          color,
          waitMs,
          processMs,
          stageStartedAt: Date.now(),
        };

        return [...prev.slice(-8), job];
      });
    }, 900);

    return () => {
      clearInterval(spawn);
    };
  }, []);

  // Single 100ms tick: update time and advance job stages
  React.useEffect(() => {
    const interval = window.setInterval(() => {
      const nowTs = Date.now();
      setNow(nowTs);
      setJobs((curr) =>
        curr.map((j) => {
          if (j.stage === 0) {
            if (nowTs - j.stageStartedAt >= j.waitMs) {
              return { ...j, stage: 1, stageStartedAt: nowTs };
            }
            return j;
          }
          if (j.stage === 1) {
            if (nowTs - j.stageStartedAt >= j.processMs) {
              return { ...j, stage: 2, stageStartedAt: nowTs };
            }
            return j;
          }
          return j;
        }),
      );
    }, 100);
    return () => clearInterval(interval);
  }, []);

  const cols = [
    {
      icon: <ClockIcon className="size-4" />,
      title: 'Waiting',
      stage: 0,
      count: jobs.filter((j) => j.stage === 0).length,
    },
    {
      icon: <PlayIcon className="size-4" />,
      title: 'Active',
      stage: 1,
      count: jobs.filter((j) => j.stage === 1).length,
    },
    {
      icon: <CheckIcon className="size-4" />,
      title: 'Completed',
      stage: 2,
      count: jobs.filter((j) => j.stage === 2).pop()?.id || 0,
    },
  ];

  return (
    <div className="border grid grod-cols-2 md:grid-cols-3 gap-4 p-4 rounded-md h-full min-h-[400px]">
      {cols.map((col, colIdx) => (
        <div key={col.title} className="flex flex-col gap-3">
          <div className="flex items-center gap-2 justify-between">
            <div className="text-muted-foreground text-sm font-medium flex items-center gap-2">
              {col.icon}
              {col.title}
            </div>
            <div className="text-muted-foreground text-sm font-medium">
              {col.count}
            </div>
          </div>
          <div className="relative flex-1 overflow-hidden rounded-sm border min-h-[100px]">
            <motion.div layout className="absolute inset-0 p-3">
              <AnimatePresence mode="sync" initial={false}>
                {jobs
                  .filter((j) => j.stage === (colIdx as 0 | 1 | 2))
                  .slice(-6)
                  .map((j, idx) => {
                    const elapsed = now - j.stageStartedAt;
                    const duration =
                      j.stage === 0
                        ? j.waitMs
                        : j.stage === 1
                          ? j.processMs
                          : 1;
                    const progress = Math.max(
                      0,
                      Math.min(1, elapsed / duration),
                    );
                    return (
                      <motion.div
                        key={j.id}
                        layout
                        initial={{ opacity: 0, y: -8 }}
                        animate={{ opacity: 1, y: 0 }}
                        exit={{ opacity: 0, x: 12 }}
                        transition={{
                          duration: 0.25,
                          delay: (j.id % 5) * 0.05 + idx * 0.02,
                        }}
                        className="flex items-center gap-2 rounded-sm border px-2 py-1 mb-2 bg-background border-foreground/70"
                      >
                        <div className="flex items-center gap-2">
                          <span
                            className="inline-block h-2 w-2 rounded-full shrink-0"
                            style={{ backgroundColor: j.color }}
                          />
                          <span className="text-xs font-medium whitespace-nowrap">
                            {j.label}
                          </span>
                        </div>
                        <div className="ml-auto w-20 h-1.5 rounded-sm bg-muted overflow-hidden">
                          <div
                            className="h-full bg-foreground"
                            style={{
                              width: `${Math.round(progress * 100)}%`,
                              transition: 'width 100ms linear',
                            }}
                          />
                        </div>
                      </motion.div>
                    );
                  })}
              </AnimatePresence>
            </motion.div>
          </div>
        </div>
      ))}
    </div>
  );
}
