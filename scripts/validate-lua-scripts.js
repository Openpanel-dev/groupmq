#!/usr/bin/env node

/**
 * Lua Script Syntax Validator
 * Validates all Lua scripts for syntax errors before deployment
 */

import { execSync } from 'child_process';
import { readdirSync, readFileSync, unlinkSync, writeFileSync } from 'fs';
import { join } from 'path';

const LUA_DIR = 'src/lua';
const REDIS_CLI = 'redis-cli';

console.log('🔍 Validating Lua scripts for syntax errors...\n');

// Get all Lua files
const luaFiles = readdirSync(LUA_DIR)
  .filter((file) => file.endsWith('.lua'))
  .sort();

let hasErrors = false;

for (const file of luaFiles) {
  const filePath = join(LUA_DIR, file);
  const content = readFileSync(filePath, 'utf8');

  console.log(`📄 Validating ${file}...`);

  try {
    // Write content to temporary file to avoid shell escaping issues
    const tempFile = `/tmp/validate-${file}`;
    writeFileSync(tempFile, content);

    // Use redis-cli to validate the script with dummy arguments
    // This ensures the script is actually compiled, not just parsed
    const result = execSync(`${REDIS_CLI} --eval ${tempFile} dummy`, {
      encoding: 'utf8',
      stdio: 'pipe',
      timeout: 5000,
    });

    // Clean up temp file
    unlinkSync(tempFile);

    // Check if the result contains error messages
    if (
      result.includes('ERR Error compiling script') ||
      result.includes('syntax error')
    ) {
      throw new Error(`Lua syntax error: ${result.trim()}`);
    }

    console.log(`✅ ${file} - OK`);
  } catch (error) {
    console.log(`❌ ${file} - SYNTAX ERROR`);
    console.log(`   Error: ${error.message}`);
    hasErrors = true;
  }
}

if (hasErrors) {
  console.log('\n🚨 Lua script validation failed!');
  process.exit(1);
} else {
  console.log('\n✅ All Lua scripts are syntactically valid!');
  process.exit(0);
}
