// Keep necessary imports from the old main.rs if they are still needed for the new setup
// For example, SocketAddr, Arc, Mutex, Server, signal might still be relevant for the new service.
// However, specific service imports like MyCoordinatorService will be replaced.

use igloo_api::flight_sql_service_server::FlightSqlServiceServer; // Corrected import
use igloo_engine::QueryEngine;
use std::net::SocketAddr;
use tonic::transport::Server;
use tokio::signal; // For graceful shutdown

// Modules declared previously
mod flight_sql_service;
mod legacy_service; // Keep if other parts of the binary still use it, otherwise remove. For now, assume it might be used.

// Use the new service
use flight_sql_service::FlightSqlServiceImpl;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr: SocketAddr = "[::1]:50051".parse()?; // Using [::1] for IPv6 loopback as specified in the issue

    // 1. Create an instance of the igloo_engine::QueryEngine.
    // For now, QueryEngine::new() might be a placeholder if it needs configuration.
    // Assuming QueryEngine::new() is the correct way to instantiate it.
    let engine = QueryEngine::new();

    // 2. Create an instance of your CoordinatorService (FlightSqlServiceImpl), passing the engine to it.
    let flight_sql_service = FlightSqlServiceImpl { engine };

    // 3. Print a confirmation message to the console before serving.
    println!("Coordinator listening on {}", addr);

    // 4. Create a new tonic::transport::Server.
    // 5. Add your service to the server and call .serve(addr).await?.
    // Incorporating graceful shutdown as was present in the original main.rs
    Server::builder()
        .add_service(FlightSqlServiceServer::new(flight_sql_service)) // Use FlightSqlServiceServer
        .serve_with_shutdown(addr, async {
            signal::ctrl_c().await.expect("failed to listen for SIGINT");
            println!("Shutting down coordinator gracefully...");
        })
        .await?;

    Ok(())
}
