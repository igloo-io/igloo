use igloo_api::igloo::{
    coordinator_service_client::CoordinatorServiceClient,
    worker_service_server::WorkerServiceServer, HeartbeatInfo, WorkerInfo,
};
use std::net::SocketAddr;
use tokio::time::{sleep, Duration};
use tonic::transport::Server;
use uuid::Uuid;

mod service;
use service::MyWorkerService;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let worker_id = Uuid::new_v4().to_string();
    let default_port = 50052;
    let port = std::env::var("IGLOO_WORKER_PORT")
        .ok()
        .and_then(|p_str| p_str.parse::<u16>().ok())
        .unwrap_or(default_port);
    let worker_addr_str = format!("127.0.0.1:{}", port);
    let worker_addr: SocketAddr = worker_addr_str.parse()?;
    println!("Worker {} attempting to listen on: {}", worker_id, worker_addr); // Added print
    let coordinator_addr =
        std::env::args().nth(1).unwrap_or_else(|| "http://127.0.0.1:50051".to_string());

    // Register with coordinator
    let mut client = CoordinatorServiceClient::connect(coordinator_addr.clone()).await?;
    let info = WorkerInfo { id: worker_id.clone(), address: format!("{}", worker_addr) };
    let _ = client.register_worker(info).await?;
    println!("Worker registered with coordinator at {}", coordinator_addr);

    // Spawn heartbeat task
    let mut client2 = CoordinatorServiceClient::connect(coordinator_addr).await?;
    let worker_id2 = worker_id.clone();
    tokio::spawn(async move {
        loop {
            let heartbeat = HeartbeatInfo {
                worker_id: worker_id2.clone(),
                timestamp: chrono::Utc::now().timestamp(),
            };
            if let Err(e) = client2.send_heartbeat(heartbeat).await {
                eprintln!("Failed to send heartbeat: {}", e);
                // Optionally, implement retry/backoff here
            }
            sleep(Duration::from_secs(5)).await;
        }
    });

    // Start gRPC server with graceful shutdown
    Server::builder()
        .add_service(WorkerServiceServer::new(MyWorkerService))
        .serve_with_shutdown(worker_addr, async {
            tokio::signal::ctrl_c().await.expect("failed to listen for event");
            println!("Shutting down worker gracefully...");
        })
        .await?;
    Ok(())
}
