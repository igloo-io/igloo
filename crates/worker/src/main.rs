mod config;
use config::Settings;
mod error;
use error::WorkerError;
use igloo_api::igloo::{
    coordinator_service_client::CoordinatorServiceClient,
    worker_service_server::{WorkerService, WorkerServiceServer},
    DataForTaskRequest, DataForTaskResponse, HeartbeatInfo, TaskDefinition, TaskResult, WorkerInfo,
};
use backoff::ExponentialBackoff;
use backoff::future::retry;
use std::net::SocketAddr;
use tokio::time::{sleep, Duration};
use tonic::{transport::Server, Request, Response, Status};
use uuid::Uuid;

struct MyWorkerService;

#[tonic::async_trait]
impl WorkerService for MyWorkerService {
    async fn execute_task(
        &self,
        request: Request<TaskDefinition>,
    ) -> Result<Response<TaskResult>, Status> {
        println!(
            "Worker received ExecuteTask: {:?}",
            request.get_ref().task_id
        );
        Ok(Response::new(TaskResult {
            task_id: request.get_ref().task_id.clone(),
            result: vec![],
        }))
    }
    async fn get_data_for_task(
        &self,
        request: Request<DataForTaskRequest>,
    ) -> Result<Response<DataForTaskResponse>, Status> {
        println!(
            "Worker received GetDataForTask: {:?}",
            request.get_ref().task_id
        );
        Ok(Response::new(DataForTaskResponse { data: vec![] }))
    }
}

#[tokio::main]
async fn main() -> Result<(), WorkerError> {
    let settings = Settings::new()?;
    let worker_id = Uuid::new_v4().to_string();
    let worker_addr = settings.worker_server_address()?;
    // let coordinator_addr = settings.coordinator_address.clone(); // Keep for reference if needed, but new logic clones settings.coordinator_address

    // Register with coordinator
    let coordinator_address_clone_for_connect = settings.coordinator_address.clone();
    let worker_id_clone_for_register = worker_id.clone();
    let worker_addr_str_for_register = settings.worker_server_address()?.to_string();

    let mut client = retry(ExponentialBackoff::default(), || async {
        println!("Attempting to connect to coordinator at {}...", coordinator_address_clone_for_connect);
        CoordinatorServiceClient::connect(coordinator_address_clone_for_connect.clone())
            .await
            .map_err(|e| {
                eprintln!("Failed to connect to coordinator: {}. Retrying...", e);
                backoff::Error::transient(WorkerError::ClientConnection(e))
            })
    }).await.map_err(|e| {
        eprintln!("Failed to connect to coordinator after multiple retries: {}", e);
        WorkerError::ConnectionFailed
    })?;
    println!("Successfully connected to coordinator at {}", settings.coordinator_address);

    let info = WorkerInfo {
        id: worker_id_clone_for_register.clone(),
        address: worker_addr_str_for_register,
    };

    retry(ExponentialBackoff::default(), || async {
        println!("Attempting to register worker {}...", info.id);
        let mut temp_client = client.clone();
        temp_client.register_worker(Request::new(info.clone())).await
            .map_err(|e| {
                eprintln!("Failed to register worker {}: {}. Retrying...", info.id, e);
                backoff::Error::transient(WorkerError::RpcError(e))
            })
    }).await.map_err(|e| {
        eprintln!("Failed to register worker {} after multiple retries: {}", info.id, e);
        WorkerError::RegistrationFailed
    })?;
    println!("Worker {} registered successfully with coordinator.", worker_id);

    // Spawn heartbeat task
    let heartbeat_coordinator_addr = settings.coordinator_address.clone();
    let worker_id_for_heartbeat = worker_id.clone();
    let heartbeat_interval = settings.heartbeat_interval_secs;

    let mut heartbeat_client = client.clone();
    tokio::spawn(async move {
        loop {
            let heartbeat_info = HeartbeatInfo {
                worker_id: worker_id_for_heartbeat.clone(),
                timestamp: chrono::Utc::now().timestamp(),
            };
            match heartbeat_client.send_heartbeat(Request::new(heartbeat_info.clone())).await {
                Ok(_) => println!("Sent heartbeat for worker {}", worker_id_for_heartbeat),
                Err(e) => { // e is tonic::Status
                    eprintln!("Failed to send heartbeat for worker {}: {}. Attempting to reconnect and resend...", worker_id_for_heartbeat, WorkerError::RpcError(e));
                    match CoordinatorServiceClient::connect(heartbeat_coordinator_addr.clone()).await {
                        Ok(new_client) => {
                            heartbeat_client = new_client;
                            eprintln!("Reconnected heartbeat client for worker {}", worker_id_for_heartbeat);
                        }
                        Err(reconnect_err) => { // reconnect_err is tonic::transport::Error
                            eprintln!("Failed to reconnect heartbeat client for worker {}: {}", worker_id_for_heartbeat, WorkerError::ClientConnection(reconnect_err));
                        }
                    }
                }
            }
            sleep(Duration::from_secs(heartbeat_interval)).await;
        }
    });

    // Start gRPC server
    Server::builder()
        .add_service(WorkerServiceServer::new(MyWorkerService))
        .serve(worker_addr)
        .await
        .map_err(|e| WorkerError::Internal(format!("Worker gRPC server failed: {}", e)))?;
    Ok(())
}
