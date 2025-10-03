// Generic logger interface that works with different logger implementations
export interface LoggerInterface {
  warn(...args: any[]): void;
  info(...args: any[]): void;
  error(...args: any[]): void;
  debug(...args: any[]): void;
}

// Default logger implementation
export class Logger implements LoggerInterface {
  constructor(
    private readonly enabled: boolean,
    private readonly name: string,
  ) {}

  debug(...args: any[]) {
    if (this.enabled) {
      console.debug(`[${this.name}]`, ...args);
    }
  }

  info(...args: any[]) {
    if (this.enabled) {
      console.log(`[${this.name}]`, ...args);
    }
  }

  warn(...args: any[]) {
    if (this.enabled) {
      console.warn(`⚠️ [${this.name}]`, ...args);
    }
  }

  error(...args: any[]) {
    if (this.enabled) {
      console.error(`💥 [${this.name}]`, ...args);
    }
  }
}
