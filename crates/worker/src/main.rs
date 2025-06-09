use igloo_api::igloo::{
    coordinator_service_client::CoordinatorServiceClient,
    worker_service_server::WorkerServiceServer, HeartbeatInfo, WorkerInfo,
};
use std::env; // Added
use std::net::SocketAddr;
use tokio::time::{sleep, Duration};
use tonic::transport::Server;
use uuid::Uuid;

mod service;
use service::MyWorkerService;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let worker_id = Uuid::new_v4().to_string();

    // Determine port from environment variable, default to 50051
    let port = env::var("PORT")
        .ok()
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(50051);

    let worker_addr_str = format!("0.0.0.0:{}", port);
    let worker_addr: SocketAddr = worker_addr_str.parse()
        .expect("Failed to parse worker address"); // Or handle error more gracefully

    println!("Worker {} will listen on {}", worker_id, worker_addr); // Updated log message

    let coordinator_addr =
        env::args().nth(1).unwrap_or_else(|| "http://127.0.0.1:50051".to_string());

    // Register with coordinator
    // Note: The address used for registration should be externally reachable if not on localhost.
    // For now, using the listen address. This might need adjustment in a containerized/NAT environment.
    let mut client = CoordinatorServiceClient::connect(coordinator_addr.clone()).await?;
    let info = WorkerInfo { id: worker_id.clone(), address: worker_addr.to_string() };
    let _ = client.register_worker(info).await?;
    println!("Worker {} registered with coordinator at {}", worker_id, coordinator_addr); // Added worker_id to log

    // Spawn heartbeat task
    let mut client2 = CoordinatorServiceClient::connect(coordinator_addr.clone()).await?; // Added clone for coordinator_addr
    let worker_id_hb = worker_id.clone(); // Use a distinct variable name for clarity
    tokio::spawn(async move {
        loop {
            let heartbeat = HeartbeatInfo {
                worker_id: worker_id_hb.clone(),
                timestamp: chrono::Utc::now().timestamp(),
            };
            if let Err(e) = client2.send_heartbeat(heartbeat).await {
                eprintln!("Worker {} failed to send heartbeat: {}", worker_id_hb, e); // Added worker_id to log
                // Optionally, implement retry/backoff here
            }
            sleep(Duration::from_secs(5)).await;
        }
    });

    println!("Worker {} starting gRPC server on {}", worker_id, worker_addr); // Added log before server start
    // Start gRPC server with graceful shutdown
    Server::builder()
        .add_service(WorkerServiceServer::new(MyWorkerService))
        .serve_with_shutdown(worker_addr, async {
            tokio::signal::ctrl_c().await.expect("failed to listen for event");
            println!("Worker {} shutting down gracefully...", worker_id); // Added worker_id to log
        })
        .await?;
    Ok(())
}
