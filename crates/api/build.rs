// TODO: Set up tonic-build for generating gRPC stubs

fn main() {
    tonic_build::configure()
        .build_server(true)
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile(
            &[
                "proto/coordinator.proto",
                "proto/client_flight.proto",
                "proto/arrow/flight/protocol/flight.proto",
                "proto/arrow/flight/protocol/sql/flight_sql.proto",
            ],
            &["proto", "proto/arrow/flight/protocol", "proto/arrow/flight/protocol/sql"],
        )
        .unwrap();
}
