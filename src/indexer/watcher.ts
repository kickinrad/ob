/**
 * Recursive vault watcher.
 *
 * Watches one vault root, applies the ignore rules from the
 * spec, and exposes a per-path debounce so a flurry of writes coalesces into
 * one `upsert` event. The 250 ms window matches the spec; tests inject a
 * shorter window (the default still tests with real wall-clock waits since
 * the fixed budget is forgiving).
 *
 * Renames surface as a removal and an upsert. Letting both flow through
 * keeps the pipeline stateless.
 *
 * The watcher subscribes to filesystem `error` events and surfaces the
 * latest error via `lastError()`; callers (the indexer facade) can poll
 * for and log them. Without this, EACCES/ENOSPC/etc. would silently break
 * the watcher with no observable signal.
 */

import { type Dirent, type Stats, readdirSync, statSync, watch as watchNative } from "node:fs";
import { join, relative, sep } from "node:path";
import type { Logger } from "../log.ts";
import { isMarkdownFile, shouldIgnorePath } from "./scanner.ts";

export type WatcherEventKind = "upsert" | "remove";

export interface WatcherEvent {
  readonly kind: WatcherEventKind;
  readonly absPath: string;
  readonly relPath: string;
}

export interface WatcherOptions {
  readonly debounceMs?: number;
  /** Override native filesystem operations at the OS boundary in tests. */
  readonly watchImpl?: typeof watchNative;
  readonly statImpl?: (path: string) => Stats;
  readonly readDirImpl?: (dir: string) => Dirent[];
  /** Override `setTimeout`/`clearTimeout` — tests inject a fake clock. */
  readonly setTimer?: (cb: () => void, ms: number) => unknown;
  readonly clearTimer?: (handle: unknown) => void;
  /**
   * Optional logger — when supplied, watcher `error` events are forwarded
   * via `logger.warn(...)`. The indexer facade always passes its logger
   * through; tests that don't care about the error path can omit it.
   */
  readonly logger?: Logger;
  /**
   * Optional callback invoked for every watcher `error`. Tests use this
   * to assert the wiring; the indexer facade uses it to track
   * `lastError` for `/metrics` exposure.
   */
  readonly onError?: (err: Error) => void;
}

export interface WatcherHandle {
  /** Resolves once watches are attached. */
  ready(): Promise<void>;
  /** Most recent filesystem watch error, if any. */
  lastError(): Error | null;
  stop(): Promise<void>;
}

const DEFAULT_DEBOUNCE_MS = 250;

/**
 * Start watching `root`. Each emitted event is the *coalesced* tail of any
 * burst of writes/removes for the same path; the handler is called with the
 * latest decision.
 */
export function startWatcher(
  root: string,
  onEvent: (ev: WatcherEvent) => void,
  opts: WatcherOptions = {},
): WatcherHandle {
  const debounceMs = opts.debounceMs ?? DEFAULT_DEBOUNCE_MS;
  const setTimer = opts.setTimer ?? ((cb, ms) => setTimeout(cb, ms));
  const clearTimer = opts.clearTimer ?? ((h) => clearTimeout(h as ReturnType<typeof setTimeout>));
  const watchImpl = opts.watchImpl ?? watchNative;
  const statImpl = opts.statImpl ?? statSync;
  const readDirImpl =
    opts.readDirImpl ?? ((dir: string) => readdirSync(dir, { withFileTypes: true }));

  interface Pending {
    kind: WatcherEventKind;
    timer: unknown;
  }
  const pending = new Map<string, Pending>();

  const flush = (relPath: string, absPath: string): void => {
    const cur = pending.get(relPath);
    if (cur === undefined) return;
    pending.delete(relPath);
    onEvent({ kind: cur.kind, absPath, relPath });
  };

  const queue = (kind: WatcherEventKind, absPath: string): void => {
    const rel = relative(root, absPath).split(sep).join("/");
    if (!isMarkdownFile(rel)) return;
    const cur = pending.get(rel);
    if (cur !== undefined) clearTimer(cur.timer);
    const timer = setTimer(() => flush(rel, absPath), debounceMs);
    pending.set(rel, { kind, timer });
  };

  let lastError: Error | null = null;
  const onError = (err: unknown): void => {
    const e = err instanceof Error ? err : new Error(String(err));
    lastError = e;
    if (opts.logger !== undefined) {
      opts.logger.warn("indexer: watcher error", { error: e.message });
    }
    if (opts.onError !== undefined) opts.onError(e);
  };

  const nativeWatches = new Map<string, ReturnType<typeof watchNative>>();
  const knownFiles = new Set<string>();
  const attachDir = (dir: string, emitExisting: boolean): void => {
    if (nativeWatches.has(dir)) return;
    let native: ReturnType<typeof watchNative>;
    try {
      native = watchImpl(dir, (_event, filename) => {
        if (filename === null) return;
        const absPath = join(dir, filename.toString());
        const relPath = relative(root, absPath);
        if (shouldIgnorePath(relPath)) return;
        try {
          const stat = statImpl(absPath);
          if (stat.isDirectory()) {
            attachDir(absPath, true);
          } else if (stat.isFile() && isMarkdownFile(relPath)) {
            knownFiles.add(absPath);
            queue("upsert", absPath);
          }
        } catch (error) {
          if ((error as NodeJS.ErrnoException).code !== "ENOENT") {
            onError(error);
            return;
          }
          for (const path of knownFiles) {
            if (path === absPath || path.startsWith(`${absPath}${sep}`)) {
              knownFiles.delete(path);
              queue("remove", path);
            }
          }
          for (const [path, handle] of nativeWatches) {
            if (path === absPath || path.startsWith(`${absPath}${sep}`)) {
              handle.close();
              nativeWatches.delete(path);
            }
          }
        }
      });
    } catch (error) {
      onError(error);
      return;
    }
    native.on("error", onError);
    nativeWatches.set(dir, native);
    let entries: Dirent[];
    try {
      entries = readDirImpl(dir);
    } catch (error) {
      onError(error);
      native.close();
      nativeWatches.delete(dir);
      return;
    }
    for (const entry of entries) {
      const path = join(dir, entry.name);
      const rel = relative(root, path);
      if (shouldIgnorePath(rel)) continue;
      if (entry.isDirectory()) attachDir(path, emitExisting);
      else if (entry.isFile() && isMarkdownFile(rel)) {
        knownFiles.add(path);
        if (emitExisting) queue("upsert", path);
      }
    }
  };
  // Bun's per-file fs.watch retains deleted-file descriptors. Watch only
  // directories, keeping handles proportional to folders instead of notes.
  attachDir(root, false);

  const readyPromise = Promise.resolve();

  return {
    ready(): Promise<void> {
      return readyPromise;
    },
    lastError(): Error | null {
      return lastError;
    },
    async stop(): Promise<void> {
      // Fire any pending events before tearing down so a write that came
      // in during a graceful shutdown still gets indexed if the indexer's
      // pipeline still accepts it.
      for (const [rel, p] of pending.entries()) {
        clearTimer(p.timer);
        // Best-effort: we can't reconstruct absPath without re-joining, but
        // Paths use platform separators, so reconstruct with the same ones.
        onEvent({
          kind: p.kind,
          absPath: `${root}${sep}${rel.split("/").join(sep)}`,
          relPath: rel,
        });
      }
      pending.clear();
      for (const native of nativeWatches.values()) native.close();
      nativeWatches.clear();
    },
  };
}
