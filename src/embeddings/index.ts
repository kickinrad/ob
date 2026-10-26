/**
 * Embedding-provider abstraction.
 *
 * Both the isolated Transformers.js provider and the OpenAI HTTP provider
 * implement the same `Embedder` interface so the indexer can stay
 * provider-agnostic. The indexer never new's a provider directly; it calls
 * `buildEmbedder(cfg, deps?)` which selects from `cfg.embeddingProvider`.
 *
 * `dim` MUST match the vector size produced by `embed()`. The indexer's
 * dimension-mismatch check on table open compares this against the on-disk
 * schema and refuses to open a table whose vectors have a different size.
 */

import type { Config } from "../config/index.ts";
import { type IsolatedTransformersDeps, buildIsolatedTransformersEmbedder } from "./isolated.ts";
import { type OpenAIDeps, buildOpenAIEmbedder } from "./openai.ts";
import { type TransformersDeps, buildTransformersEmbedder } from "./transformers.ts";

export interface Embedder {
  /** Vector dimension produced by `embed()`. Constant for the lifetime of the instance. */
  readonly dim: number;
  embed(texts: string[]): Promise<Float32Array[]>;
  /** Release a provider's subprocess or connection during indexer shutdown. */
  stop?(): Promise<void>;
}

/**
 * Thrown by provider misuse — e.g. embedding empty input, OpenAI provider
 * built without an API key, or unrecoverable HTTP failure after backoff.
 */
export class EmbedderError extends Error {
  // Keeps EmbedderError aligned with the closed-set codes in `src/errors.ts`
  // without creating a layering cycle (the embedder module pre-dates the
  // shared error base). Both adapters translate this code to their
  // transport-specific 502 envelope.
  readonly code = "embedder_failed" as const;
  constructor(message: string) {
    super(message);
    this.name = "EmbedderError";
  }
}

export interface BuildEmbedderDeps {
  readonly transformers?: TransformersDeps;
  readonly isolated?: IsolatedTransformersDeps;
  readonly openai?: OpenAIDeps;
}

/**
 * Build the configured embedder. Pure factory — model files / network sockets
 * are lazy-initialised inside the provider on the first `embed()` call.
 */
export function buildEmbedder(cfg: Config, deps: BuildEmbedderDeps = {}): Embedder {
  if (cfg.embeddingProvider === "openai") {
    if (cfg.openaiApiKey === undefined) {
      // Belt-and-suspenders: `loadConfig` already enforces this, but a
      // misconfigured test that constructs a Config by hand would hit
      // here, and a typed error beats a `fetch` 401.
      throw new EmbedderError("OPENAI_API_KEY required when EMBEDDING_PROVIDER=openai");
    }
    const openaiInput: {
      apiKey: string;
      model: string;
      baseUrl?: string;
    } = { apiKey: cfg.openaiApiKey, model: cfg.embeddingModel };
    if (cfg.openaiBaseUrl !== undefined) openaiInput.baseUrl = cfg.openaiBaseUrl;
    return buildOpenAIEmbedder(openaiInput, deps.openai);
  }
  const input = { model: cfg.embeddingModel, dataDir: cfg.dataDir };
  // Unit tests inject a pipeline; production isolates ONNX in a recyclable
  // child so native allocations cannot accumulate in the server process.
  if (deps.transformers !== undefined) return buildTransformersEmbedder(input, deps.transformers);
  return buildIsolatedTransformersEmbedder(input, deps.isolated);
}

export type { OpenAIDeps } from "./openai.ts";
export type { TransformersDeps } from "./transformers.ts";
