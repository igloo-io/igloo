// TODO: Set up tonic-build for generating gRPC stubs

fn main() {
    tonic_build::configure()
        .build_server(true)
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile_protos(
            &[
                "proto/coordinator.proto",
                "proto/client_flight.proto",
                "proto/arrow/flight/protocol/flight.proto",
                "proto/arrow/flight/sql/flight_sql.proto",
            ],
            &["proto", "proto/arrow/flight/protocol", "proto/arrow/flight/sql"],
        )
        .unwrap();

    // Patch generated file to allow clippy::doc_overindented_list_items
    let out_dir = std::env::var("OUT_DIR").unwrap();
    let gen_file = std::path::Path::new(&out_dir).join("arrow.flight.protocol.rs");
    if let Ok(mut contents) = std::fs::read_to_string(&gen_file) {
        if !contents.contains("clippy::doc_overindented_list_items") {
            // Insert #[allow(...)] before every struct, enum, or trait
            let mut new_lines = Vec::new();
            for line in contents.lines() {
                let trimmed = line.trim_start();
                if trimmed.starts_with("pub struct ")
                    || trimmed.starts_with("pub enum ")
                    || trimmed.starts_with("pub trait ")
                {
                    new_lines.push("#[allow(clippy::doc_overindented_list_items)]");
                }
                new_lines.push(line);
            }
            contents = new_lines.join("\n");
            let _ = std::fs::write(&gen_file, contents);
        }
    }
}
