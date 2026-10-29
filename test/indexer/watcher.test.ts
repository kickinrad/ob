import { afterEach, describe, expect, test } from "bun:test";
import { EventEmitter } from "node:events";
import {
  mkdirSync,
  mkdtempSync,
  readdirSync,
  rmSync,
  statSync,
  unlinkSync,
  type watch,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { type WatcherEvent, startWatcher } from "../../src/indexer/watcher.ts";

function tmpRoot(): string {
  return mkdtempSync(join(tmpdir(), "ob-watcher-test-"));
}

const cleanup: (() => Promise<void>)[] = [];
afterEach(async () => {
  while (cleanup.length > 0) {
    const fn = cleanup.pop();
    if (fn !== undefined) await fn();
  }
});

function collect(): { events: WatcherEvent[]; on: (e: WatcherEvent) => void } {
  const events: WatcherEvent[] = [];
  return {
    events,
    on: (e) => {
      events.push(e);
    },
  };
}

function fakeNativeWatch() {
  const listeners = new Map<string, (event: string, filename: string | null) => void>();
  const handles = new Map<string, EventEmitter>();
  const closed = new Set<string>();
  const watchImpl = ((dir: string, listener: (event: string, filename: string | null) => void) => {
    listeners.set(dir, listener);
    const handle = Object.assign(new EventEmitter(), { close: () => closed.add(dir) });
    handles.set(dir, handle);
    return handle;
  }) as unknown as typeof watch;
  return {
    watchImpl,
    closed,
    emitFile: (dir: string, filename: string): void => listeners.get(dir)?.("change", filename),
    emitError: (dir: string, error: unknown): void => {
      handles.get(dir)?.emit("error", error);
    },
  };
}

async function waitMs(ms: number): Promise<void> {
  await new Promise<void>((r) => setTimeout(r, ms));
}

async function waitUntil(predicate: () => boolean, timeoutMs = 3000, pollMs = 25): Promise<void> {
  const start = Date.now();
  while (!predicate()) {
    if (Date.now() - start > timeoutMs) throw new Error("waitUntil timeout");
    await waitMs(pollMs);
  }
}

describe("startWatcher (real fs)", () => {
  test("ignores hidden directories like .obsidian/", async () => {
    const root = tmpRoot();
    const sink = collect();
    const w = startWatcher(root, sink.on, { debounceMs: 50 });
    cleanup.push(async () => w.stop());
    await w.ready();
    mkdirSync(join(root, ".obsidian"), { recursive: true });
    writeFileSync(join(root, ".obsidian", "workspace.json"), "{}");
    await waitMs(400);
    expect(sink.events).toEqual([]);
  });

  test("ignores non-markdown files", async () => {
    const root = tmpRoot();
    const sink = collect();
    const w = startWatcher(root, sink.on, { debounceMs: 50 });
    cleanup.push(async () => w.stop());
    await w.ready();
    writeFileSync(join(root, "x.txt"), "not md");
    await waitMs(400);
    expect(sink.events).toEqual([]);
  });

  test("emits one upsert event for a new markdown file", async () => {
    const root = tmpRoot();
    const sink = collect();
    const w = startWatcher(root, sink.on, { debounceMs: 50 });
    cleanup.push(async () => w.stop());
    await w.ready();
    writeFileSync(join(root, "note.md"), "# H");
    await waitUntil(() => sink.events.length > 0);
    expect(sink.events).toHaveLength(1);
    expect(sink.events[0]?.kind).toBe("upsert");
    expect(sink.events[0]?.relPath).toBe("note.md");
  });

  test("debounce coalesces 5 rapid writes into one event", async () => {
    const root = tmpRoot();
    const sink = collect();
    const w = startWatcher(root, sink.on, { debounceMs: 75 });
    cleanup.push(async () => w.stop());
    await w.ready();
    const target = join(root, "x.md");
    for (let i = 0; i < 5; i++) {
      writeFileSync(target, `content ${i}`);
      await waitMs(10);
    }
    await waitUntil(() => sink.events.length > 0, 1000);
    await waitMs(150);
    expect(sink.events.length).toBe(1);
    expect(sink.events[0]?.kind).toBe("upsert");
  });

  test("unlink emits a remove event", async () => {
    const root = tmpRoot();
    const sink = collect();
    const w = startWatcher(root, sink.on, { debounceMs: 50 });
    cleanup.push(async () => w.stop());
    await w.ready();
    const f = join(root, "y.md");
    writeFileSync(f, "x");
    await waitUntil(() => sink.events.length > 0, 2000);
    sink.events.length = 0;
    unlinkSync(f);
    await waitUntil(() => sink.events.length > 0, 2000);
    expect(sink.events[0]?.kind).toBe("remove");
  });

  test("emits one shared ready promise", async () => {
    const root = tmpRoot();
    const w = startWatcher(root, () => undefined, { debounceMs: 50 });
    cleanup.push(async () => w.stop());
    const a = w.ready();
    const b = w.ready();
    expect(a).toBe(b);
    await a;
  });

  test("nested-directory writes are observed", async () => {
    const root = tmpRoot();
    const sink = collect();
    const w = startWatcher(root, sink.on, { debounceMs: 50 });
    cleanup.push(async () => w.stop());
    await w.ready();
    mkdirSync(join(root, "sub"), { recursive: true });
    writeFileSync(join(root, "sub", "z.md"), "x");
    await waitUntil(() => sink.events.length > 0);
    expect(sink.events[0]?.relPath).toBe("sub/z.md");
  });

  test("stop flushes pending events before tearing down", async () => {
    const root = tmpRoot();
    const sink = collect();
    // Use a fake clock so the debounce timer never fires by itself; force
    // the flush to happen via stop().
    const clock: { fired: number; pending: Array<() => void> } = { fired: 0, pending: [] };
    const w = startWatcher(root, sink.on, {
      debounceMs: 60_000,
      setTimer: (cb) => {
        clock.pending.push(cb);
        return clock.pending.length - 1;
      },
      clearTimer: () => undefined,
    });
    cleanup.push(async () => undefined);
    await w.ready();
    writeFileSync(join(root, "fl.md"), "x");
    await waitMs(80);
    // No event yet — the fake debounce timer hasn't fired.
    expect(sink.events).toEqual([]);
    await w.stop();
    // After stop, the queued event was flushed synchronously through
    // onEvent. Our `setTimer` returned IDs, not real timers, so the event
    // is the queued payload — kind: upsert.
    expect(sink.events.length).toBe(1);
    expect(sink.events[0]?.kind).toBe("upsert");
    expect(sink.events[0]?.relPath).toBe("fl.md");
  });

  test("clean shutdown when no pending events", async () => {
    const root = tmpRoot();
    const sink = collect();
    const w = startWatcher(root, sink.on, { debounceMs: 50 });
    await w.ready();
    await w.stop();
    expect(sink.events).toEqual([]);
  });

  test("native watch errors reach lastError, logger, and callback", async () => {
    const root = tmpRoot();
    const sink = collect();
    let captured: Error | null = null;
    const logEntries: string[] = [];
    const logger = {
      trace: () => undefined,
      debug: () => undefined,
      info: () => undefined,
      warn: (msg: string): void => {
        logEntries.push(msg);
      },
      error: () => undefined,
    };
    const fake = fakeNativeWatch();
    const w = startWatcher(root, sink.on, {
      watchImpl: fake.watchImpl,
      logger,
      onError: (e) => {
        captured = e;
      },
    });
    cleanup.push(async () => w.stop());
    const ENOSPC = new Error("ENOSPC: no space");
    fake.emitError(root, ENOSPC);
    expect(captured).not.toBeNull();
    expect((captured as unknown as Error).message).toBe("ENOSPC: no space");
    expect(w.lastError()?.message).toBe("ENOSPC: no space");
    expect(logEntries).toContain("indexer: watcher error");
  });

  test("non-Error error payload is wrapped in Error", async () => {
    const root = tmpRoot();
    const fake = fakeNativeWatch();
    const w = startWatcher(root, () => undefined, { watchImpl: fake.watchImpl });
    cleanup.push(async () => w.stop());
    fake.emitError(root, "string error");
    expect(w.lastError()?.message).toBe("string error");
  });

  test("stat errors other than ENOENT preserve a known note", async () => {
    const root = tmpRoot();
    const note = join(root, "note.md");
    writeFileSync(note, "existing");
    const sink = collect();
    const errors: Error[] = [];
    const fake = fakeNativeWatch();
    const failure = Object.assign(new Error("permission denied"), { code: "EACCES" });
    const w = startWatcher(root, sink.on, {
      watchImpl: fake.watchImpl,
      statImpl: (path) => {
        if (path === note) throw failure;
        return statSync(path);
      },
      onError: (error) => errors.push(error),
    });
    cleanup.push(async () => w.stop());
    fake.emitFile(root, "note.md");
    expect(errors).toEqual([failure]);
    expect(w.lastError()).toBe(failure);
    expect(sink.events).toEqual([]);
  });

  test("a failed native watch reports the directory error and keeps other watches", async () => {
    const root = tmpRoot();
    const sub = join(root, "sub");
    mkdirSync(sub);
    const errors: Error[] = [];
    const sink = collect();
    const fake = fakeNativeWatch();
    const failure = Object.assign(new Error("watch limit"), { code: "ENOSPC" });
    const watchImpl = ((
      dir: string,
      listener: (event: string, filename: string | null) => void,
    ) => {
      if (dir === sub) throw failure;
      return fake.watchImpl(dir, listener);
    }) as typeof watch;
    const w = startWatcher(root, sink.on, { watchImpl, onError: (error) => errors.push(error) });
    cleanup.push(async () => w.stop());
    expect(errors).toEqual([failure]);
    writeFileSync(join(root, "good.md"), "ok");
    fake.emitFile(root, "good.md");
    await waitUntil(() => sink.events.length > 0);
    expect(sink.events[0]?.relPath).toBe("good.md");
  });

  test("a failed directory read reports the error and closes that watch", async () => {
    const root = tmpRoot();
    const sub = join(root, "sub");
    mkdirSync(sub);
    const errors: Error[] = [];
    const fake = fakeNativeWatch();
    const failure = Object.assign(new Error("directory vanished"), { code: "ENOENT" });
    const w = startWatcher(root, () => undefined, {
      watchImpl: fake.watchImpl,
      readDirImpl: (dir) => {
        if (dir === sub) throw failure;
        return readdirSync(dir, { withFileTypes: true });
      },
      onError: (error) => errors.push(error),
    });
    cleanup.push(async () => w.stop());
    expect(errors).toEqual([failure]);
    expect(fake.closed.has(sub)).toBe(true);
  });

  test("removing a directory closes its native watch", async () => {
    const root = tmpRoot();
    const sub = join(root, "sub");
    mkdirSync(sub);
    writeFileSync(join(sub, "note.md"), "# Note\n");
    const fake = fakeNativeWatch();
    const sink = collect();
    const w = startWatcher(root, sink.on, { watchImpl: fake.watchImpl, debounceMs: 10 });
    cleanup.push(async () => w.stop());
    rmSync(sub, { recursive: true });
    fake.emitFile(root, "sub");
    expect(fake.closed.has(sub)).toBe(true);
    await waitUntil(() => sink.events.length > 0);
    expect(sink.events).toMatchObject([{ kind: "remove", relPath: "sub/note.md" }]);
  });

  test("lastError() returns null before any error fires", async () => {
    const root = tmpRoot();
    const w = startWatcher(root, () => undefined, { debounceMs: 50 });
    cleanup.push(async () => w.stop());
    expect(w.lastError()).toBeNull();
  });
});
