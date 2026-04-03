use std::io::Result;

fn main() -> Result<()> {
    // Proto files are at lance-fork/protos/ (2 levels up from rust/yata-wasm/)
    let proto_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../protos");
    let proto_dir = proto_dir.canonicalize().unwrap_or_else(|_| proto_dir.clone());
    let proto_str = proto_dir.to_str().unwrap();

    let mut prost_build = prost_build::Config::new();
    prost_build.protoc_arg("--experimental_allow_proto3_optional");
    prost_build.extern_path(".lance.encodings", "::lance_encoding::format::pb");
    prost_build.compile_protos(
        &[
            &format!("{}/file2.proto", proto_str),
            &format!("{}/table.proto", proto_str),
        ],
        &[proto_str],
    )?;
    Ok(())
}
