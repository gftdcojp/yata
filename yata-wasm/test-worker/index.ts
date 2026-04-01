import wasmModule from "../lance-fork/pkg/yata_wasm_bg.wasm";
import { initSync, probe, encode_vertex_lance, read_lance_footer } from "../lance-fork/pkg/yata_wasm.js";

export default {
  async fetch(request: Request): Promise<Response> {
    initSync({ module: wasmModule });

    const url = new URL(request.url);

    if (url.pathname === "/probe") {
      return Response.json({ result: probe() });
    }

    if (url.pathname === "/encode") {
      try {
        const lance_bytes = encode_vertex_lance(
          ["Post", "Post", "Profile"],
          ["rkey1", "rkey2", "rkey3"],
          ["did:web:alice", "did:web:bob", "did:web:carol"],
          ["rkey1", "rkey2", "rkey3"],
          ['{"text":"hello"}', '{"text":"world"}', '{"displayName":"Carol"}'],
        );

        // Read back the footer to verify it's a valid Lance file
        const footer = JSON.parse(read_lance_footer(lance_bytes));

        return Response.json({
          ok: true,
          'lance_file_bytes': lance_bytes.length,
          footer,
          'has_magic': true,
        });
      } catch (e: any) {
        return Response.json({ error: e.message || String(e) }, { status: 500 });
      }
    }

    return Response.json({
      endpoints: ["/probe", "/encode"],
      description: "lance-core + lance-encoding + lance-file-assembly in Cloudflare Workers WASM",
    });
  },
};
