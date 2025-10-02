#!/usr/bin/env node

import { copyFileSync, existsSync, mkdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const rootDir = join(__dirname, '../..');
const publicDir = join(__dirname, '../public');

// Ensure public directory exists
if (!existsSync(publicDir)) {
  mkdirSync(publicDir, { recursive: true });
}

// Copy benchmark JSON files
const benchmarkFiles = ['groupmq.json', 'bullmq.json'];

for (const file of benchmarkFiles) {
  const source = join(rootDir, 'benchmark/results', file);
  const dest = join(publicDir, file);

  if (existsSync(source)) {
    copyFileSync(source, dest);
    console.log(`✓ Copied ${file} to website/public/`);
  } else {
    console.warn(`⚠ Warning: ${file} not found at ${source}`);
  }
}

console.log('✓ Benchmark files copied successfully');
