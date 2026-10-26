/** Bun IPC entry point for the recyclable ONNX embedding process. */
import { buildTransformersEmbedder } from "./transformers.ts";

interface Request {
  id: number;
  model: string;
  dataDir: string;
  texts: string[];
}

let embedder: ReturnType<typeof buildTransformersEmbedder> | null = null;
process.on("message", (message: unknown) => {
  const request = message as Request;
  if (embedder === null) {
    embedder = buildTransformersEmbedder({ model: request.model, dataDir: request.dataDir });
  }
  const active = embedder;
  void active.embed(request.texts).then(
    (vectors) => process.send?.({ id: request.id, dim: active.dim, vectors }),
    (error) => process.send?.({ id: request.id, error: String(error) }),
  );
});
