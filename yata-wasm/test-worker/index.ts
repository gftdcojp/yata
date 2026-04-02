import wasmModule from "../lance-fork/pkg/yata_wasm_bg.wasm";
import { initSync, probe, encodeVertexLance, readLanceFooter } from "../lance-fork/pkg/yata_wasm.js";

export default {
  async fetch(request: Request): Promise<Response> {
    initSync({ module: wasmModule });

    const url = new URL(request.url);

    if (url.pathname === "/probe") {
      return Response.json({ result: probe() });
    }

    if (url.pathname === "/encode") {
      try {
        const lanceBytes = encodeVertexLance(
          ["Post", "Post", "Profile"],
          ["rkey1", "rkey2", "rkey3"],
          ["did:web:alice", "did:web:bob", "did:web:carol"],
          ["rkey1", "rkey2", "rkey3"],
          ['{"text":"hello"}', '{"text":"world"}', '{"displayName":"Carol"}'],
        );

        // Read back the footer to verify it's a valid Lance file
        const footer = JSON.parse(readLanceFooter(lanceBytes));

        return Response.json({
          ok: true,
          'lanceFileBytes': lanceBytes.length,
          footer,
          'hasMagic': true,
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
