import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import type Redis from 'ioredis';

export type ScriptName =
  | 'enqueue'
  | 'reserve'
  | 'reserve-batch'
  | 'reserve-atomic'
  | 'complete'
  | 'complete-and-reserve-next'
  | 'retry'
  | 'heartbeat'
  | 'cleanup'
  | 'promote-delayed-jobs'
  | 'promote-delayed-one'
  | 'change-delay'
  | 'get-active-count'
  | 'get-waiting-count'
  | 'get-delayed-count'
  | 'get-active-jobs'
  | 'get-waiting-jobs'
  | 'get-delayed-jobs'
  | 'get-unique-groups'
  | 'get-unique-groups-count'
  | 'promote-buffered-groups'
  | 'recover-delayed-groups'
  | 'cleanup-poisoned-group'
  | 'remove'
  | 'clean-status'
  | 'is-empty'
  | 'dead-letter';

const cacheByClient = new WeakMap<Redis, Map<ScriptName, string>>();

function scriptPath(name: ScriptName): string {
  // Resolve Lua script path in both dev (TS) and prod (dist) builds.
  const currentDir = path.dirname(fileURLToPath(import.meta.url));

  const candidates = [
    // Dev: alongside loader.ts (src/lua/<name>.lua)
    path.join(currentDir, `${name}.lua`),
    // Prod: dist/lua/<name>.lua adjacent to built bundle directory
    path.join(currentDir, 'lua', `${name}.lua`),
  ];

  for (const candidate of candidates) {
    if (fs.existsSync(candidate)) return candidate;
  }
  // Fallback to first candidate; read will throw a helpful error if missing
  return candidates[0];
}

export async function loadScript(
  client: Redis,
  name: ScriptName,
): Promise<string> {
  let map = cacheByClient.get(client);
  if (!map) {
    map = new Map();
    cacheByClient.set(client, map);
  }
  const cached = map.get(name);
  if (cached) return cached;

  const file = scriptPath(name);
  const lua = fs.readFileSync(file, 'utf8');
  const sha = await (client as any).script('load', lua);
  map.set(name, sha as string);
  return sha as string;
}

export async function evalScript<T = any>(
  client: Redis,
  name: ScriptName,
  argv: Array<string>,
  numKeys = 0,
): Promise<T> {
  const sha = await loadScript(client, name);
  return (client as any).evalsha(sha, numKeys, ...argv);
}
