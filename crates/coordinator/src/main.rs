use igloo_api::igloo::coordinator_service_server::CoordinatorServiceServer;
// FlightServiceServer is imported via igloo_api now
use igloo_api::new_flight_sql_service; // Use the helper function
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::signal;
use tokio::sync::Mutex;
use tonic::transport::Server;

mod service;
use service::{ClusterState, MyCoordinatorService};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr: SocketAddr = "127.0.0.1:50051".parse()?;
    let cluster: ClusterState = Arc::new(Mutex::new(HashMap::new()));
    let svc = MyCoordinatorService { cluster };
    let flight_sql_server = new_flight_sql_service(); // Use the helper function to get a FlightServiceServer
                                                      // Start gRPC server with graceful shutdown
    println!("Coordinator listening on {}", addr);
    Server::builder()
        .add_service(CoordinatorServiceServer::new(svc)) // Existing service
        .add_service(flight_sql_server) // Add the Flight SQL service
        .serve_with_shutdown(addr, async {
            signal::ctrl_c().await.expect("failed to listen for event");
            println!("Shutting down coordinator gracefully...");
        })
        .await?;
    Ok(())
}
