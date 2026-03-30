use std::io::Result;

fn main() -> Result<()> {
    let mut prost_build = prost_build::Config::new();
    prost_build.protoc_arg("--experimental_allow_proto3_optional");
    prost_build.extern_path(".lance.encodings", "::lance_encoding::format::pb");
    prost_build.compile_protos(
        &[
            "../lance-file/protos/file2.proto",
            "../lance-file/protos/encodings.proto",
        ],
        &["../lance-file/protos"],
    )?;
    Ok(())
}
