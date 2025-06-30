// This file can be used to export a library interface for the igloo crate if needed.
// For now, it can be minimal if the primary purpose is the binary.

pub fn hello() -> String {
    "Hello from Igloo Crate!".to_string()
}

// Re-export key components from other crates if desired
// pub use igloo_core::some_function;
// pub use igloo_api::some_type;
