#!/usr/bin/env node

/**
 * Lua Script Syntax Validator
 * Validates all Lua scripts for syntax errors before deployment
 */

import { readdirSync, readFileSync } from 'fs';
import Redis from 'ioredis';
import { join } from 'path';

const LUA_DIR = 'src/lua';

console.log('🔍 Validating Lua scripts for syntax errors...\n');

// Get all Lua files
const luaFiles = readdirSync(LUA_DIR)
  .filter((file) => file.endsWith('.lua'))
  .sort();

let hasErrors = false;

// Create Redis client for validation
const redis = new Redis({
  host: 'localhost',
  port: 6379,
  connectTimeout: 5000,
  lazyConnect: true,
});

try {
  await redis.connect();
  console.log('📡 Connected to Redis for validation\n');
} catch (error) {
  console.log('⚠️  Could not connect to Redis - skipping Lua script validation');
  console.log('   This is normal if Redis is not running locally');
  console.log('   Lua scripts will be validated when Redis is available');
  process.exit(0);
}

for (const file of luaFiles) {
  const filePath = join(LUA_DIR, file);
  const content = readFileSync(filePath, 'utf8');

  console.log(`📄 Validating ${file}...`);

  try {
    // Use Redis SCRIPT LOAD to validate the script
    // This will compile the script and return an error if there are syntax issues
    await redis.script('LOAD', content);
    console.log(`✅ ${file} - OK`);
  } catch (error) {
    console.log(`❌ ${file} - SYNTAX ERROR`);
    console.log(`   Error: ${error.message}`);
    hasErrors = true;
  }
}

// Close Redis connection
await redis.quit();

if (hasErrors) {
  console.log('\n🚨 Lua script validation failed!');
  process.exit(1);
} else {
  console.log('\n✅ All Lua scripts are syntactically valid!');
  process.exit(0);
}
