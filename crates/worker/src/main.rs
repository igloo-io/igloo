use igloo_worker::run_worker_server; // Use item from own library
use std::error::Error;
use std::net::SocketAddr;
use tokio::sync::oneshot;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let worker_id = Uuid::new_v4().to_string();

    let worker_addr_str =
        std::env::var("WORKER_ADDR").unwrap_or_else(|_| "127.0.0.1:50052".to_string());
    let coordinator_addr_str =
        std::env::var("COORDINATOR_ADDR").unwrap_or_else(|_| "http://127.0.0.1:50051".to_string());

    let worker_addr: SocketAddr = worker_addr_str.parse()?;

    println!(
        "Preparing to start worker {} on {} connecting to coordinator at {}",
        worker_id, worker_addr, coordinator_addr_str
    );

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    // Clone worker_id for the server task and for the shutdown handler task
    let server_worker_id = worker_id.clone();
    let shutdown_handler_worker_id = worker_id.clone();

    // Spawn the server task
    let server_task_handle = tokio::spawn(run_worker_server(
        server_worker_id, // Moved into server task
        worker_addr,
        coordinator_addr_str,
        shutdown_rx, // shutdown_rx is moved into server task
    ));

    // Spawn the Ctrl+C handler task
    tokio::spawn(async move {
        // shutdown_tx is moved into this task
        // shutdown_handler_worker_id is moved into this task
        match tokio::signal::ctrl_c().await {
            Ok(()) => {
                println!(
                    "Ctrl+C received by worker {}, sending shutdown signal...",
                    shutdown_handler_worker_id
                );
                if shutdown_tx.send(()).is_err() {
                    eprintln!(
                        "Failed to send shutdown signal to worker {} server task.",
                        shutdown_handler_worker_id
                    );
                }
            }
            Err(err) => {
                eprintln!(
                    "Failed to listen for Ctrl+C in worker {}: {}",
                    shutdown_handler_worker_id, err
                );
            }
        }
    });

    // Await the server task to complete.
    // This ensures main doesn't exit until the server has shut down,
    // whether due to Ctrl+C or other reasons (e.g. internal error in run_worker_server).
    println!("Worker {} main function now awaiting server task completion.", worker_id);
    match server_task_handle.await {
        Ok(Ok(())) => {
            println!("Worker {} server shut down gracefully.", worker_id);
            Ok(())
        }
        Ok(Err(e)) => {
            eprintln!("Worker {} server exited with an error: {:?}", worker_id, e);
            Err(e) // e is already Box<dyn Error + Send + Sync>
        }
        Err(e) => { // This is a JoinError if the task panicked
            eprintln!("Worker {} server task panicked: {:?}", worker_id, e);
            // Convert JoinError to a Box<dyn Error + Send + Sync>
            Err(format!("Worker server task failed: {}", e).into())
        }
    }
}
