/** Keep ONNX allocations out of the long-lived vault server process. */
import { fileURLToPath } from "node:url";
import { EmbedderError } from "./index.ts";
import type { TransformersEmbedderInput } from "./transformers.ts";

const DEFAULT_BATCHES_PER_CHILD = 32;
const DEFAULT_CHILD_PATH = fileURLToPath(new URL("./transformers-worker.ts", import.meta.url));

interface ChildReply {
  id: number;
  dim?: number;
  vectors?: Float32Array[];
  error?: string;
}

export interface IsolatedTransformersDeps {
  readonly childPath?: string;
  readonly batchesPerChild?: number;
}

export function buildIsolatedTransformersEmbedder(
  input: TransformersEmbedderInput,
  deps: IsolatedTransformersDeps = {},
): {
  readonly dim: number;
  embed(texts: string[]): Promise<Float32Array[]>;
  stop(): Promise<void>;
} {
  const childPath = deps.childPath ?? DEFAULT_CHILD_PATH;
  const batchesPerChild = deps.batchesPerChild ?? DEFAULT_BATCHES_PER_CHILD;
  let child: ReturnType<typeof Bun.spawn> | null = null;
  let batches = 0;
  let dim = 0;
  let requestId = 0;
  let pending: { resolve(reply: ChildReply): void; reject(error: Error): void } | null = null;
  let queue = Promise.resolve();
  let stopped = false;
  let stopPromise: Promise<void> | null = null;

  function spawnChild(): void {
    const next = Bun.spawn([process.execPath, childPath], {
      ipc(message) {
        const waiting = pending;
        if (waiting === null) return;
        pending = null;
        waiting.resolve(message as ChildReply);
      },
      onExit(proc, code, signal) {
        if (child !== proc) return;
        child = null;
        const waiting = pending;
        if (waiting === null) return;
        pending = null;
        waiting.reject(
          new EmbedderError(`embedding child exited (code ${code}, signal ${signal})`),
        );
      },
      stdout: "ignore",
      stderr: "inherit",
    });
    child = next;
    batches = 0;
  }

  async function recycle(): Promise<void> {
    const old = child;
    child = null;
    if (old !== null) {
      old.kill();
      await old.exited;
    }
  }

  async function oneBatch(texts: string[]): Promise<Float32Array[]> {
    if (stopped) throw new EmbedderError("embedding child has stopped");
    if (child !== null && batches >= batchesPerChild) await recycle();
    if (stopped) throw new EmbedderError("embedding child stopped");
    if (child === null) spawnChild();
    const proc = child;
    if (proc === null) throw new EmbedderError("embedding child failed to start");
    const id = ++requestId;
    let reply: ChildReply;
    try {
      reply = await new Promise<ChildReply>((resolve, reject) => {
        pending = { resolve, reject };
        proc.send({ id, model: input.model, dataDir: input.dataDir, texts });
      });
    } finally {
      pending = null;
    }
    batches++;
    if (reply.id !== id) throw new EmbedderError("embedding child returned an out-of-order reply");
    if (reply.error !== undefined) throw new EmbedderError(reply.error);
    if (reply.vectors === undefined || reply.dim === undefined) {
      throw new EmbedderError("embedding child returned an incomplete reply");
    }
    dim = reply.dim;
    return reply.vectors;
  }

  function embed(texts: string[]): Promise<Float32Array[]> {
    if (texts.length === 0) return Promise.resolve([]);
    const result = queue.then(() => oneBatch(texts));
    queue = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
  }

  function stop(): Promise<void> {
    if (stopPromise !== null) return stopPromise;
    stopped = true;
    const waiting = pending;
    pending = null;
    waiting?.reject(new EmbedderError("embedding child stopped"));
    stopPromise = (async () => {
      await recycle();
      await queue;
    })();
    return stopPromise;
  }

  return {
    get dim() {
      return dim;
    },
    embed,
    stop,
  };
}
