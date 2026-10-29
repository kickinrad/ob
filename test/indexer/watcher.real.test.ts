import { expect, test } from "bun:test";
import { mkdtempSync, readdirSync, readlinkSync, renameSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { startWatcher } from "../../src/indexer/watcher.ts";

test("replacing and deleting synced notes does not accumulate file descriptors", async () => {
  if (process.platform !== "linux") return;
  const root = mkdtempSync(join(tmpdir(), "ob-watch-fd-"));
  const watcher = startWatcher(root, () => {});
  try {
    await watcher.ready();
    for (let i = 0; i < 80; i++) {
      const note = join(root, `note-${i % 12}.md`);
      const replacement = join(root, `incoming-${i}.md`);
      writeFileSync(replacement, `# Note ${i}\n`);
      rmSync(note, { force: true });
      renameSync(replacement, note);
      await Bun.sleep(10);
    }
    for (let i = 0; i < 12; i++) rmSync(join(root, `note-${i}.md`), { force: true });
    const deletedHandles = (): string[] =>
      readdirSync("/proc/self/fd").filter((fd) => {
        try {
          const target = readlinkSync(`/proc/self/fd/${fd}`);
          return target.startsWith(root) && target.endsWith(" (deleted)");
        } catch {
          return false;
        }
      });
    await Bun.sleep(500);
    for (let i = 0; i < 20 && deletedHandles().length > 0; i++) await Bun.sleep(100);
    expect(deletedHandles().length).toBeLessThanOrEqual(1);
  } finally {
    await watcher.stop();
    rmSync(root, { recursive: true, force: true });
  }
});
