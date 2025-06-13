// Import the FlightSqlServiceServer
use anyhow::Error;
use arrow_flight::flight_service_server::FlightServiceServer as FlightSqlServiceServer;
use igloo_engine::QueryEngine;
use service::CoordinatorService; // Your new service
use std::net::SocketAddr;
use tokio::signal;
use tonic::transport::Server;

// Reference the service module
mod service;

#[tokio::main]
async fn main() -> Result<(), Error> {
    // Define the server address
    let addr: SocketAddr = "[::1]:50051".parse()?;

    // Create an instance of the QueryEngine
    // This assumes QueryEngine::new() is the correct way to instantiate it.
    // Adjust if QueryEngine has a different constructor.
    let engine = QueryEngine::new().await?;

    // Create an instance of CoordinatorService
    let coordinator_service = CoordinatorService { engine };

    // Print confirmation message
    println!("Coordinator listening on {}", addr);

    // Setup and run the gRPC server
    Server::builder()
        // Add the FlightSqlService implementation
        .add_service(FlightSqlServiceServer::new(coordinator_service))
        .serve_with_shutdown(addr, async {
            signal::ctrl_c().await.expect("failed to listen for event");
            println!("Shutting down coordinator gracefully...");
        })
        .await?;

    Ok(())
}
