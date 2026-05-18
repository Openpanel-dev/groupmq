import { describe, expect, it } from 'vitest';
import { evalScript } from '../src/lua/loader';

describe('Lua script loader', () => {
  it('reloads and retries once when Redis returns NOSCRIPT', async () => {
    let loadCount = 0;
    const evalshaCalls: string[] = [];
    const redis = {
      script: async (command: string) => {
        expect(command).toBe('load');
        loadCount += 1;
        return `sha-${loadCount}`;
      },
      evalsha: async (sha: string) => {
        evalshaCalls.push(sha);
        if (evalshaCalls.length === 1) {
          throw new Error('NOSCRIPT No matching script. Please use EVAL.');
        }
        return 'ok';
      },
    };

    await expect(
      evalScript(redis as any, 'enqueue', ['test-namespace'], 1),
    ).resolves.toBe('ok');

    expect(loadCount).toBe(2);
    expect(evalshaCalls).toEqual(['sha-1', 'sha-2']);
  });

  it('does not retry non-NOSCRIPT Redis errors', async () => {
    let loadCount = 0;
    const redis = {
      script: async () => {
        loadCount += 1;
        return `sha-${loadCount}`;
      },
      evalsha: async () => {
        throw new Error('READONLY You cannot write against a read only replica.');
      },
    };

    await expect(
      evalScript(redis as any, 'enqueue', ['test-namespace'], 1),
    ).rejects.toThrow('READONLY');

    expect(loadCount).toBe(1);
  });
});
