// TODO: Set up tonic-build for generating gRPC stubs

fn main() {
    tonic_build::configure()
        .build_server(true)
        .compile(
            &[
                "proto/coordinator.proto",
                "proto/client_flight.proto",
                "proto/arrow/flight/protocol/flight.proto",
                "proto/arrow/flight/protocol/schema.proto",
                "proto/arrow/flight/protocol/type.proto",
                "proto/arrow/flight/protocol/message.proto",
                "proto/arrow/flight/protocol/keyvalue.proto",
            ],
            &["proto", "proto/arrow/flight/protocol"]
        )
        .unwrap();
}
