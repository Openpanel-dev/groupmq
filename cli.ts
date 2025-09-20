#!/usr/bin/env node

import Redis from 'ioredis';
import { Queue } from './src/queue';

// ANSI color codes for terminal output
const colors = {
  reset: '\x1b[0m',
  bright: '\x1b[1m',
  dim: '\x1b[2m',
  red: '\x1b[31m',
  green: '\x1b[32m',
  yellow: '\x1b[33m',
  blue: '\x1b[34m',
  magenta: '\x1b[35m',
  cyan: '\x1b[36m',
  white: '\x1b[37m',
};

interface QueueStats {
  active: number;
  waiting: number;
  delayed: number;
  total: number;
  'unique-groups': number;
  groups: string[];
  timestamp: Date;
}

type CliMode = 'monitor' | 'list-completed' | 'list-failed' | 'list-both';

class QueueMonitor {
  private queue: Queue;
  private redis: Redis;
  private namespace: string;
  private pollInterval: number;
  private isRunning = false;
  private intervalId?: NodeJS.Timeout;

  constructor(redisUrl: string, namespace: string, pollInterval = 1000) {
    this.redis = new Redis(redisUrl);
    this.namespace = namespace;
    this.pollInterval = pollInterval;
    this.queue = new Queue({
      redis: this.redis,
      namespace,
    });
  }

  private formatNumber(num: number): string {
    return num.toString().padStart(6, ' ');
  }

  private formatTime(): string {
    return new Date().toLocaleTimeString();
  }

  private clearScreen(): void {
    process.stdout.write('\x1b[2J\x1b[H');
  }

  private displayHeader(): void {
    console.log(
      `${colors.bright}${colors.cyan}╔════════════════════════════════════════════════════════════════════╗${colors.reset}`,
    );
    console.log(
      `${colors.bright}${colors.cyan}║                          GroupMQ Monitor                           ║${colors.reset}`,
    );
    console.log(
      `${colors.bright}${colors.cyan}╚════════════════════════════════════════════════════════════════════╝${colors.reset}`,
    );
    console.log();
    console.log(
      `${colors.dim}Namespace: ${colors.reset}${colors.yellow}${this.namespace}${colors.reset}`,
    );
    console.log(
      `${colors.dim}Update frequency (cli): ${colors.reset}${colors.yellow}${this.pollInterval}ms${colors.reset}`,
    );
    console.log(
      `${colors.dim}Last Update: ${colors.reset}${colors.white}${this.formatTime()}${colors.reset}`,
    );
    console.log();
  }

  private displayStats(stats: QueueStats): void {
    // Job counts section
    console.log(`${colors.bright}${colors.white}Job Counts:${colors.reset}`);
    console.log(
      `${colors.cyan}  Active:      ${colors.reset}${colors.green}${this.formatNumber(stats.active)}${colors.reset}`,
    );
    console.log(
      `${colors.cyan}  Waiting:     ${colors.reset}${colors.yellow}${this.formatNumber(stats.waiting)}${colors.reset}`,
    );
    console.log(
      `${colors.cyan}  Delayed:     ${colors.reset}${colors.magenta}${this.formatNumber(stats.delayed)}${colors.reset}`,
    );
    console.log(
      `${colors.cyan}  Total:       ${colors.reset}${colors.bright}${this.formatNumber(stats.total)}${colors.reset}`,
    );
    console.log();

    // Groups section
    console.log(`${colors.bright}${colors.white}Groups:${colors.reset}`);
    console.log(
      `${colors.cyan}  Unique Groups: ${colors.reset}${colors.blue}${this.formatNumber(stats['unique-groups'])}${colors.reset}`,
    );
    console.log();

    // Groups list (limited to first 10 for display)
    if (stats.groups.length > 0) {
      console.log(
        `${colors.bright}${colors.white}Active Groups:${colors.reset}`,
      );
      const displayGroups = stats.groups.slice(0, 10);

      displayGroups.forEach((group, index) => {
        const prefix = index === displayGroups.length - 1 ? '└─' : '├─';
        console.log(
          `${colors.dim}  ${prefix} ${colors.reset}${colors.white}${group}${colors.reset}`,
        );
      });

      if (stats.groups.length > 10) {
        console.log(
          `${colors.dim}     ... and ${stats.groups.length - 10} more${colors.reset}`,
        );
      }
    } else {
      console.log(`${colors.dim}  No active groups${colors.reset}`);
    }

    console.log();
    console.log(`${colors.dim}Press Ctrl+C to exit${colors.reset}`);
  }

  private async fetchStats(): Promise<QueueStats> {
    try {
      const [counts, groups] = await Promise.all([
        this.queue.getJobCounts(),
        this.queue.getUniqueGroups(),
      ]);

      return {
        total:
          counts.active +
          counts.waiting +
          counts.delayed +
          counts.completed +
          counts.failed +
          counts.paused +
          counts['waiting-children'] +
          counts.prioritized,
        ...counts,
        'unique-groups': groups.length,
        groups: groups.sort(),
        timestamp: new Date(),
      };
    } catch (error) {
      console.error(`${colors.red}Error fetching stats:${colors.reset}`, error);
      throw error;
    }
  }

  private async updateDisplay(): Promise<void> {
    try {
      const stats = await this.fetchStats();
      this.clearScreen();
      this.displayHeader();
      this.displayStats(stats);
    } catch (error) {
      console.error(
        `${colors.red}Failed to update display:${colors.reset}`,
        error,
      );
    }
  }

  async start(): Promise<void> {
    if (this.isRunning) return;

    this.isRunning = true;
    console.log(`${colors.green}Starting GroupMQ Monitor...${colors.reset}`);

    // Test connection
    try {
      await this.redis.ping();
      console.log(`${colors.green}Connected to Redis${colors.reset}`);
    } catch (error) {
      console.error(
        `${colors.red}Failed to connect to Redis:${colors.reset}`,
        error,
      );
      return;
    }

    // Initial display
    await this.updateDisplay();

    // Set up polling
    this.intervalId = setInterval(async () => {
      if (this.isRunning) {
        await this.updateDisplay();
      }
    }, this.pollInterval);

    // Handle Ctrl+C gracefully
    process.on('SIGINT', () => {
      this.stop();
    });

    process.on('SIGTERM', () => {
      this.stop();
    });
  }

  stop(): void {
    if (!this.isRunning) return;

    this.isRunning = false;
    if (this.intervalId) {
      clearInterval(this.intervalId);
    }

    console.log(`\n${colors.yellow}Stopping monitor...${colors.reset}`);
    this.redis.quit();
    console.log(`${colors.green}Monitor stopped${colors.reset}`);
    process.exit(0);
  }
}

// CLI interface
function showHelp(): void {
  console.log(`
${colors.bright}${colors.cyan}GroupMQ Monitor CLI${colors.reset}

${colors.bright}Usage:${colors.reset}
  npx tsx cli.ts [options]
  npx tsx cli.ts --list-completed [--limit 20] -n <namespace>
  npx tsx cli.ts --list-failed [--limit 20] -n <namespace>

${colors.bright}Options:${colors.reset}
  --redis-url, -r     Redis connection URL (default: redis://127.0.0.1:6379)
  --namespace, -n     Queue namespace (required)
  --interval, -i      Poll interval in milliseconds (default: 1000)
  --list-completed    List recent completed jobs and exit
  --list-failed       List recent failed jobs and exit
  --limit             Number of items to list (default: 20)
  --help, -h          Show this help

${colors.bright}Examples:${colors.reset}
  npx tsx cli.ts -n myqueue
  npx tsx cli.ts -n myqueue -r redis://localhost:6379 -i 2000
  npx tsx cli.ts --namespace myqueue --interval 500
  npx tsx cli.ts -n myqueue --list-completed --limit 10
  npx tsx cli.ts -n myqueue --list-failed
`);
}

// Parse command line arguments
function parseArgs(): {
  redisUrl: string;
  namespace: string;
  interval: number;
  mode: CliMode;
  limit: number;
} | null {
  const args = process.argv.slice(2);

  let redisUrl = 'redis://127.0.0.1:6379';
  let namespace = '';
  let interval = 1000;
  let mode: CliMode = 'monitor';
  let limit = 20;

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    const next = args[i + 1];

    switch (arg) {
      case '--help':
      case '-h':
        showHelp();
        return null;

      case '--redis-url':
      case '-r':
        if (!next) {
          console.error(
            `${colors.red}Error: --redis-url requires a value${colors.reset}`,
          );
          return null;
        }
        redisUrl = next;
        i++;
        break;

      case '--namespace':
      case '-n':
        if (!next) {
          console.error(
            `${colors.red}Error: --namespace requires a value${colors.reset}`,
          );
          return null;
        }
        namespace = next;
        i++;
        break;

      case '--interval':
      case '-i':
        if (!next) {
          console.error(
            `${colors.red}Error: --interval requires a value${colors.reset}`,
          );
          return null;
        }
        {
          const parsed = Number.parseInt(next, 10);
          if (Number.isNaN(parsed) || parsed < 100) {
            console.error(
              `${colors.red}Error: --interval must be a number >= 100${colors.reset}`,
            );
            return null;
          }
          interval = parsed;
          i++;
          break;
        }

      case '--list-completed':
        mode = mode === 'list-failed' ? 'list-both' : 'list-completed';
        break;

      case '--list-failed':
        mode = mode === 'list-completed' ? 'list-both' : 'list-failed';
        break;

      case '--limit':
        if (!next) {
          console.error(
            `${colors.red}Error: --limit requires a value${colors.reset}`,
          );
          return null;
        }
        {
          const parsed = Number.parseInt(next, 10);
          if (Number.isNaN(parsed) || parsed < 1) {
            console.error(
              `${colors.red}Error: --limit must be a positive number${colors.reset}`,
            );
            return null;
          }
          limit = parsed;
          i++;
          break;
        }

      default:
        console.error(
          `${colors.red}Error: Unknown argument: ${arg}${colors.reset}`,
        );
        showHelp();
        return null;
    }
  }

  if (!namespace) {
    console.error(`${colors.red}Error: --namespace is required${colors.reset}`);
    showHelp();
    return null;
  }

  return { redisUrl, namespace, interval, mode, limit };
}

// Main execution
async function main(): Promise<void> {
  const config = parseArgs();
  if (!config) return;

  // Listing modes
  if (config.mode !== 'monitor') {
    const redis = new Redis(config.redisUrl);
    const queue = new Queue({ redis, namespace: config.namespace });

    const printDivider = () =>
      console.log(
        `${colors.dim}────────────────────────────────────────────────────────────${colors.reset}`,
      );
    const fmtDate = (ms?: number) => (ms ? new Date(ms).toLocaleString() : '-');
    const truncate = (s: any, n = 120) => {
      const str = typeof s === 'string' ? s : JSON.stringify(s);
      return str.length > n ? str.slice(0, n - 1) + '…' : str;
    };

    try {
      if (config.mode === 'list-completed' || config.mode === 'list-both') {
        const items = await queue.getCompleted(config.limit);
        console.log(
          `${colors.bright}${colors.green}Last Completed (${items.length})${colors.reset}`,
        );
        printDivider();
        if (items.length === 0) {
          console.log(`${colors.dim}No completed jobs found.${colors.reset}`);
        }
        for (const j of items) {
          console.log(
            `${colors.cyan}${j.id}${colors.reset}  ` +
              `${colors.dim}group:${colors.reset}${j.groupId}  ` +
              `${colors.dim}finished:${colors.reset}${fmtDate(j.finishedOn)}  ` +
              `${colors.dim}attempts:${colors.reset}${j.attempts}/${j.maxAttempts}`,
          );
          console.log(
            `${colors.dim}payload:${colors.reset} ${truncate(j.payload)}
${colors.dim}return :${colors.reset} ${truncate(j.returnvalue)}`,
          );
          printDivider();
        }
      }

      if (config.mode === 'list-failed' || config.mode === 'list-both') {
        const items = await queue.getFailed(config.limit);
        console.log(
          `${colors.bright}${colors.red}Last Failed (${items.length})${colors.reset}`,
        );
        printDivider();
        if (items.length === 0) {
          console.log(`${colors.dim}No failed jobs found.${colors.reset}`);
        }
        for (const j of items) {
          console.log(
            `${colors.cyan}${j.id}${colors.reset}  ` +
              `${colors.dim}group:${colors.reset}${j.groupId}  ` +
              `${colors.dim}finished:${colors.reset}${fmtDate(j.finishedOn)}  ` +
              `${colors.dim}attempts:${colors.reset}${j.attempts}/${j.maxAttempts}`,
          );
          console.log(
            `${colors.dim}payload:${colors.reset} ${truncate(j.payload)}
${colors.dim}reason :${colors.reset} ${truncate(j.failedReason)}${j.stacktrace ? `\n${colors.dim}stack  :${colors.reset} ${truncate(j.stacktrace, 240)}` : ''}`,
          );
          printDivider();
        }
      }
    } catch (error) {
      console.error(`${colors.red}Error listing jobs:${colors.reset}`, error);
      process.exit(1);
    } finally {
      await redis.quit();
    }

    process.exit(0);
  }

  // Default monitor mode
  const monitor = new QueueMonitor(
    config.redisUrl,
    config.namespace,
    config.interval,
  );

  try {
    await monitor.start();
  } catch (error) {
    console.error(
      `${colors.red}Failed to start monitor:${colors.reset}`,
      error,
    );
    process.exit(1);
  }
}

// Run if called directly (ESM version)
if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch((error) => {
    console.error(`${colors.red}Unhandled error:${colors.reset}`, error);
    process.exit(1);
  });
}

export { QueueMonitor };
