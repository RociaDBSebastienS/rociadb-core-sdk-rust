// EN: Build script to generate gRPC client code from protos.
// FR: Script de build pour generer le code gRPC depuis les protos.
use std::env;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(env::var("OUT_DIR")?);
    let descriptor_path = out_dir.join("service_descriptor.bin");
    let mut includes = vec!["proto"];
    for candidate in ["/usr/include", "/usr/local/include"] {
        if std::path::Path::new(candidate).exists() {
            includes.push(candidate);
        }
    }

    tonic_prost_build::configure()
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .compile_well_known_types(true)
        .build_server(false)
        .extern_path(".google.protobuf.Empty", "::pbjson_types::Empty")
        .extern_path(".google.protobuf.Timestamp", "::pbjson_types::Timestamp")
        .extern_path(".google.protobuf.Struct", "::pbjson_types::Struct")
        .file_descriptor_set_path(&descriptor_path)
        .compile_protos(&["proto/upstream/v1/upstream.proto"], &includes)?;

    println!("cargo:rerun-if-changed=proto/upstream/v1/upstream.proto");
    println!("cargo:rerun-if-changed=build.rs");
    Ok(())
}
