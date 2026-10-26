process.on("SIGTERM", () => {
  setTimeout(() => process.exit(0), 75);
});

process.on("message", (message: unknown) => {
  const request = message as { id: number; texts: string[] };
  if (request.texts.includes("crash")) process.exit(7);
  if (request.texts.includes("hang")) return;
  if (request.texts.includes("incomplete")) {
    process.send?.({ id: request.id });
    return;
  }
  if (request.texts.includes("error")) {
    process.send?.({ id: request.id, error: `model broke in ${process.pid}` });
    return;
  }
  process.send?.({
    id: request.id,
    dim: 2,
    vectors: request.texts.map((text) => new Float32Array([process.pid, text.length])),
  });
});
