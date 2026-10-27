# 0018: Bounded Local Embedder Memory

## Summary

Keep the default Transformers.js embedder's memory bounded while a vault is being edited. ONNX Runtime's CPU arena is turned off, every output tensor is disposed after its vectors are copied out, and local inference moves into a short-lived Bun child process that the indexer recycles after at most 8 batches of at most 16 texts each. `@huggingface/transformers` is updated from 4.2.0 to 4.3.1. Implements the revised topology rule in [Architecture › Server-Process Topology](../specs/architecture/index.md#server-process-topology) and the new memory constraint in [Vault Indexer › Constraints](../specs/vault-indexer/index.md#constraints).

**Spec:** [Vault Indexer](../specs/vault-indexer/)
**Status:** complete
**Depends On:** 0003

## Motivation

A server indexing a 785-note vault reached its 4 GiB soft memory limit during ordinary edits. In a 122-second edit burst the in-process embedder grew to 10,380 MiB RSS. ONNX's CPU arena keeps its high-water allocation after each inference, output tensors were never disposed, and a large note sent hundreds of chunks to one inference call, so memory grew with every burst and never came back.

## Requirements

### Testing Requirements

This change MUST satisfy the project's standing testing rules (see [Architecture › Testing & Lint](../specs/architecture/index.md#testing--lint)):

- The 100% line coverage gate on `src/` MUST hold under `bun run test:cov`.
- The worker lifecycle (batch splitting, recycling, child exit, stop during a request or a recycle) MUST be tested against a real Bun child through a fixture worker, not the ONNX model.
- Tensor disposal MUST be asserted after vectors are copied out.
- Biome MUST pass with the project config; `bunx tsc --noEmit` MUST pass.

## Design

### Approach

- `realLoadPipeline` passes `session_options: { enableCpuMemArena: false }` so ONNX returns memory after each inference.
- `buildTransformersEmbedder` disposes the output tensor in a `finally` once its vectors are sliced out.
- `buildIsolatedTransformersEmbedder` (`src/embeddings/isolated.ts`) runs the Transformers.js embedder in `src/embeddings/transformers-worker.ts` over Bun IPC. It splits each `embed()` call into batches of at most 16 texts, runs them one at a time, and replaces the child after 8 batches. A child exit fails the in-flight request with `EmbedderError`; the next request starts a fresh child.
- `Embedder` gains an optional `stop()`. The indexer calls it on shutdown and when startup fails after the embedder warms, so no child outlives the server.
- `buildEmbedder` uses the isolated embedder by default; tests that inject a Transformers pipeline keep the in-process path.

### Decisions

- **Decision:** Recycle a child process instead of relying on the arena setting and disposal alone.
  - **Why:** The arena setting and disposal release what Transformers.js exposes, but ONNX's native allocations are invisible to Bun. Ending the process is the only release the server fully controls.
- **Decision:** Cap each inference at 16 texts.
  - **Why:** ONNX allocates for the whole input at once, so recycling cannot bound the peak of a single large note.

### Non-Goals

- The OpenAI-compatible provider is unchanged.
- No new env vars; the batch and recycle limits are constants.

## Tasks

- [x] `src/embeddings/transformers.ts`: disable the ONNX CPU arena; dispose output tensors
- [x] `src/embeddings/isolated.ts` and `src/embeddings/transformers-worker.ts`: batched, recyclable embedding child
- [x] `src/embeddings/index.ts`: optional `Embedder.stop()`; isolated embedder by default
- [x] `src/indexer/index.ts`: stop the embedder on shutdown and on failed startup
- [x] `@huggingface/transformers` updated from 4.2.0 to 4.3.1
- [x] Tests: worker batching, recycling, exit and stop paths; tensor disposal; indexer cleanup
- [x] Specs: Architecture topology and Vault Indexer constraint, each with a Changelog row; `README.md`
- [x] This change document, `docs/index.yml`, and `docs/index.md`

## Risks / Open Questions

- **Cold-start cost per child.** Each new child loads the model from the local cache, so heavy indexing pays a model load every 8 batches. A full 785-note scan and a 488-edit burst completed with zero index errors.

## References

- Spec: [Vault Indexer › Constraints](../specs/vault-indexer/index.md#constraints), [Architecture › Server-Process Topology](../specs/architecture/index.md#server-process-topology)
- Related changes: [0003 — Vault indexer](./0003-vault-indexer.md)
- Code: `src/embeddings/isolated.ts`, `src/embeddings/transformers-worker.ts`, `src/embeddings/transformers.ts`, `src/indexer/index.ts`
