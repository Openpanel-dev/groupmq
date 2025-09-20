import { promises as fs } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

async function main() {
  const __filename = fileURLToPath(import.meta.url);
  const __dirname = path.dirname(__filename);
  const repoRoot = path.resolve(__dirname, '..');

  const srcLuaDir = path.join(repoRoot, 'src', 'lua');
  const distLuaDir = path.join(repoRoot, 'dist', 'lua');

  try {
    await fs.mkdir(distLuaDir, { recursive: true });
  } catch {}

  const entries = await fs.readdir(srcLuaDir, { withFileTypes: true });
  const copyOps = entries
    .filter((e) => e.isFile() && e.name.endsWith('.lua'))
    .map(async (e) => {
      const from = path.join(srcLuaDir, e.name);
      const to = path.join(distLuaDir, e.name);
      console.log(`Copying ${from} to ${to}`);
      await fs.copyFile(from, to);
    });

  await Promise.all(copyOps);
}

main().catch((err) => {
  console.error('[copy-lua] Failed to copy Lua scripts to dist:', err);
  process.exitCode = 1;
});
