use igloo_api::igloo::{worker_service_server::{WorkerService, WorkerServiceServer}, TaskDefinition, TaskResult, DataForTaskRequest, DataForTaskResponse, coordinator_service_client::CoordinatorServiceClient, WorkerInfo, HeartbeatInfo};
use tokio::time::{sleep, Duration};
use tonic::{transport::Server, Request, Response, Status};
use std::sync::Arc;
use std::net::SocketAddr;
use uuid::Uuid;

struct MyWorkerService;

#[tonic::async_trait]
impl WorkerService for MyWorkerService {
    async fn execute_task(&self, request: Request<TaskDefinition>) -> Result<Response<TaskResult>, Status> {
        println!("Worker received ExecuteTask: {:?}", request.get_ref().task_id);
        Ok(Response::new(TaskResult { task_id: request.get_ref().task_id.clone(), result: vec![] }))
    }
    async fn get_data_for_task(&self, request: Request<DataForTaskRequest>) -> Result<Response<DataForTaskResponse>, Status> {
        println!("Worker received GetDataForTask: {:?}", request.get_ref().task_id);
        Ok(Response::new(DataForTaskResponse { data: vec![] }))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let worker_id = Uuid::new_v4().to_string();
    let worker_addr: SocketAddr = "127.0.0.1:50052".parse()?;
    let coordinator_addr = std::env::args().nth(1).unwrap_or_else(|| "http://127.0.0.1:50051".to_string());

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
            let heartbeat = HeartbeatInfo { worker_id: worker_id2.clone(), timestamp: chrono::Utc::now().timestamp() };
            let _ = client2.send_heartbeat(heartbeat).await;
            sleep(Duration::from_secs(5)).await;
        }
    });

    // Start gRPC server
    Server::builder()
        .add_service(WorkerServiceServer::new(MyWorkerService))
        .serve(worker_addr)
        .await?;
    Ok(())
}
