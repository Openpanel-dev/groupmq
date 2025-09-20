export class Logger {
  constructor(
    private readonly enabled: boolean,
    private readonly name: string,
  ) {}
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
