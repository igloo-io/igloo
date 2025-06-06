// TODO: API crate for gRPC and Arrow Flight definitions

// Re-export the generated proto code
pub mod igloo {
    include!(concat!(env!("OUT_DIR"), "/igloo.rs"));
}
