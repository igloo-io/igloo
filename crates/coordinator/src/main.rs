use igloo_coordinator::launch_coordinator; // Use the function from the library
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("Coordinator binary starting...");
    // Call the extracted function with no address override (uses default)
    if let Err(e) = launch_coordinator(None).await {
        eprintln!("Coordinator failed to launch: {}", e);
        std::process::exit(1);
    }
    println!("Coordinator binary finished.");
    Ok(())
}
