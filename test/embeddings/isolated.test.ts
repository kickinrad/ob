import { describe, expect, test } from "bun:test";
import { fileURLToPath } from "node:url";
import { buildIsolatedTransformersEmbedder } from "../../src/embeddings/isolated.ts";

const fixture = fileURLToPath(new URL("../fixtures/embedder-child.ts", import.meta.url));

describe("isolated Transformers embedder", () => {
  test("recycles the child after two batches while keeping results and dimensions", async () => {
    const embedder = buildIsolatedTransformersEmbedder(
      { model: "unused", dataDir: "/tmp/unused" },
      { childPath: fixture, batchesPerChild: 2 },
    );
    try {
      expect(await embedder.embed([])).toEqual([]);
      expect(embedder.dim).toBe(0);
      const first = await embedder.embed(["a"]);
      const second = await embedder.embed(["bb"]);
      const third = await embedder.embed(["ccc"]);
      expect(first[0]?.[1]).toBe(1);
      expect(second[0]?.[1]).toBe(2);
      expect(third[0]?.[1]).toBe(3);
      expect(embedder.dim).toBe(2);
      expect(first[0]?.[0]).toBe(second[0]?.[0] ?? -1);
      expect(third[0]?.[0]).not.toBe(first[0]?.[0] ?? -1);
    } finally {
      await embedder.stop();
    }
  });

  test("a crashed child fails the request and the next request starts a fresh child", async () => {
    const embedder = buildIsolatedTransformersEmbedder(
      { model: "unused", dataDir: "/tmp/unused" },
      { childPath: fixture, batchesPerChild: 2 },
    );
    try {
      await expect(embedder.embed(["crash"])).rejects.toThrow("embedding child exited");
      const vectors = await embedder.embed(["ok"]);
      expect(vectors[0]?.[1]).toBe(2);
    } finally {
      await embedder.stop();
    }
  });

  test("worker failures and incomplete replies surface as embedding errors", async () => {
    const embedder = buildIsolatedTransformersEmbedder(
      { model: "unused", dataDir: "/tmp/unused" },
      { childPath: fixture, batchesPerChild: 2 },
    );
    try {
      await expect(embedder.embed(["error"])).rejects.toThrow("model broke");
      await expect(embedder.embed(["incomplete"])).rejects.toThrow("incomplete reply");
      expect((await embedder.embed(["ok"]))[0]?.[1]).toBe(2);
    } finally {
      await embedder.stop();
    }
  });

  test("stop rejects a pending batch and reaps the child", async () => {
    const embedder = buildIsolatedTransformersEmbedder(
      { model: "unused", dataDir: "/tmp/unused" },
      { childPath: fixture, batchesPerChild: 2 },
    );
    await embedder.embed(["ready"]);
    const pending = embedder.embed(["hang"]);
    await Bun.sleep(10);
    await embedder.stop();
    await expect(pending).rejects.toThrow("stopped");
    await expect(embedder.embed(["after"])).rejects.toThrow("stopped");
  });

  test("stop during recycle cannot spawn a replacement child", async () => {
    const embedder = buildIsolatedTransformersEmbedder(
      { model: "unused", dataDir: "/tmp/unused" },
      { childPath: fixture, batchesPerChild: 1 },
    );
    try {
      await embedder.embed(["first"]);
      const rollover = embedder.embed(["second"]);
      await Bun.sleep(10);
      const stopped = embedder.stop();
      await expect(rollover).rejects.toThrow("stopped");
      await stopped;
    } finally {
      await embedder.stop();
    }
  });

  test("failed batches also count toward the recycle limit", async () => {
    const embedder = buildIsolatedTransformersEmbedder(
      { model: "unused", dataDir: "/tmp/unused" },
      { childPath: fixture, batchesPerChild: 2 },
    );
    async function failedPid(): Promise<string> {
      try {
        await embedder.embed(["error"]);
        throw new Error("expected child failure");
      } catch (error) {
        const match = String(error).match(/model broke in (\d+)/);
        if (match?.[1] === undefined) throw error;
        return match[1];
      }
    }
    try {
      const first = await failedPid();
      const second = await failedPid();
      const third = await failedPid();
      expect(first).toBe(second);
      expect(third).not.toBe(first);
    } finally {
      await embedder.stop();
    }
  });
});
