import wasmModule from "../lance-fork/pkg/yata_wasm_bg.wasm";
import { initSync, probe, encode_vertex_batch } from "../lance-fork/pkg/yata_wasm.js";

export default {
  async fetch(request: Request): Promise<Response> {
    // Initialize WASM module
    initSync({ module: wasmModule });

    const url = new URL(request.url);

    if (url.pathname === "/probe") {
      return Response.json({ result: probe() });
    }

    if (url.pathname === "/encode") {
      try {
        const encoded = encode_vertex_batch(
          ["Post", "Post"],
          ["rkey1", "rkey2"],
          ["did:web:alice", "did:web:bob"],
          ["rkey1", "rkey2"],
          ['{"text":"hello"}', '{"text":"world"}'],
        );
        return Response.json({
          ok: true,
          encoded_bytes: encoded.length,
          first_16: Array.from(encoded.slice(0, 16)),
        });
      } catch (e: any) {
        return Response.json({ error: e.message || String(e) }, { status: 500 });
      }
    }

    return Response.json({
      endpoints: ["/probe", "/encode"],
      wasm_size: "4.6 MiB",
      description: "lance-core + lance-encoding in Cloudflare Workers WASM",
    });
  },
};
