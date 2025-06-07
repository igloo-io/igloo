use igloo_coordinator::{run_server, ClusterState, QueryState}; // Use items from own library
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex};
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let addr: SocketAddr = "127.0.0.1:50051".parse()?;
    let cluster: ClusterState = Arc::new(Mutex::new(HashMap::new()));
    let queries: QueryState = Arc::new(Mutex::new(HashMap::new()));

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    // Handle Ctrl+C for graceful shutdown in main application
    tokio::spawn(async move {
        match tokio::signal::ctrl_c().await {
            Ok(()) => {
                println!("Ctrl+C received, sending shutdown signal to server...");
                if shutdown_tx.send(()).is_err() {
                    eprintln!("Failed to send shutdown signal to server task.");
                }
            }
            Err(err) => {
                eprintln!("Failed to listen for Ctrl+C: {}", err);
            }
        }
    });

    println!("Starting coordinator server...");
    run_server(addr, cluster, queries, shutdown_rx).await
}
