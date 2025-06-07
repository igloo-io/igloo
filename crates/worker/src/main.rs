use igloo_worker::run_worker_server; // Use item from own library
use std::net::SocketAddr;
use tokio::sync::oneshot;
use uuid::Uuid;
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let worker_id = Uuid::new_v4().to_string();

    // Use environment variables for addresses or fall back to defaults
    let worker_addr_str = std::env::var("WORKER_ADDR").unwrap_or_else(|_| "127.0.0.1:50052".to_string());
    let coordinator_addr_str = std::env::var("COORDINATOR_ADDR").unwrap_or_else(|_| "http://127.0.0.1:50051".to_string());

    let worker_addr: SocketAddr = worker_addr_str.parse()?;

    println!(
        "Starting worker {} on {} connecting to coordinator at {}",
        worker_id, worker_addr, coordinator_addr_str
    );

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let worker_id_for_signal_handler = worker_id.clone(); // Clone for the signal handler

    // Handle Ctrl+C for graceful shutdown
    tokio::spawn(async move {
        match tokio::signal::ctrl_c().await {
            Ok(()) => {
                println!("Ctrl+C received by worker {}, sending shutdown signal...", worker_id_for_signal_handler);
                if shutdown_tx.send(()).is_err() {
                    eprintln!("Failed to send shutdown signal to worker server task.");
                }
            }
            Err(err) => {
                eprintln!("Failed to listen for Ctrl+C in worker: {}", err);
            }
        }
    });

    run_worker_server(worker_id.clone(), worker_addr, coordinator_addr_str, shutdown_rx).await
}
