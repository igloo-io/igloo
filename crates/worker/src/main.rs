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
use std::time::Duration as StdDuration; // For ExponentialBackoff config
use std::error::Error; // For .source()
use tokio::time::{sleep, Duration}; // Keep tokio's Duration for sleep
use chrono::Utc; // Added for timestamp logging
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
    // Note: worker_id_clone_for_register and worker_addr_str_for_register are effectively replaced by direct use or re-creation.

    let connect_backoff_settings = ExponentialBackoff {
        max_elapsed_time: Some(StdDuration::from_secs(60)), // Existing
        randomization_factor: 0.5,                         // Updated
        multiplier: 1.5,                                   // Added/Updated
        max_interval: StdDuration::from_secs(15),          // Updated
        ..ExponentialBackoff::default()
    };

    let mut client = retry(connect_backoff_settings.clone(), || {
        println!(
            "[{}] Attempting to connect to coordinator at {}...",
            chrono::Utc::now().to_rfc3339(), // Added timestamp
            settings.coordinator_address
        );
        let coordinator_address_clone = settings.coordinator_address.clone();
        async move {
            CoordinatorServiceClient::connect(coordinator_address_clone)
                .await
                .map_err(|e_original| {
                    let worker_error = WorkerError::ClientConnection(e_original);
                    eprintln!(
                        "[{}] Failed to connect to coordinator: {}. Retrying...",
                        chrono::Utc::now().to_rfc3339(), // Added timestamp
                        worker_error
                    );
                    backoff::Error::transient(worker_error)
                })
        }
    })
    .await
    .map_err(|e| { // e is WorkerError::ClientConnection
        eprintln!(
            "[{}] Final connection failure to coordinator after multiple retries: {}",
            chrono::Utc::now().to_rfc3339(), // Added timestamp
            e
        );
        WorkerError::ConnectionFailed
    })?;
    println!("Successfully connected to coordinator at {}", settings.coordinator_address);

    let info_for_retry = WorkerInfo {
        id: worker_id.clone(),
        address: settings.worker_server_address()
                       .expect("Invalid worker server address for registration info")
                       .to_string(),
    };

    let register_backoff_settings = ExponentialBackoff {
        max_elapsed_time: Some(StdDuration::from_secs(60)), // Existing
        randomization_factor: 0.5,                         // Updated
        multiplier: 1.5,                                   // Added/Updated
        max_interval: StdDuration::from_secs(15),          // Updated
        ..ExponentialBackoff::default()
    };

    retry(register_backoff_settings.clone(), || {
        println!(
            "[{}] Attempting to register worker {}...",
            chrono::Utc::now().to_rfc3339(), // Added timestamp
            info_for_retry.id
        );
        let mut temp_client = client.clone();
        let current_info = info_for_retry.clone();
        async move {
            temp_client.register_worker(Request::new(current_info))
                .await
                .map_err(|e_original| {
                    let worker_error = WorkerError::RpcError(e_original);
                    eprintln!(
                        "[{}] Failed to register worker {}: {}. Retrying...",
                        chrono::Utc::now().to_rfc3339(), // Added timestamp
                        info_for_retry.id,
                        worker_error
                    );
                    backoff::Error::transient(worker_error)
                })
        }
    })
    .await
    .map_err(|e| { // e is WorkerError::RpcError
        eprintln!(
            "[{}] Final registration failure for worker {} after multiple retries: {}",
            chrono::Utc::now().to_rfc3339(), // Added timestamp
            info_for_retry.id,
            e
        );
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
                Ok(_) => {
                    println!( // Added timestamp
                        "[{}] Sent heartbeat for worker {}",
                        chrono::Utc::now().to_rfc3339(),
                        worker_id_for_heartbeat
                    );
                }
                Err(e) => { // e is tonic::Status
                    let rpc_error = WorkerError::RpcError(e);
                    eprintln!(
                        "[{}] Failed to send heartbeat for worker {}. Error: {}. Retrying...", // Ensure it ends with "Retrying..."
                        chrono::Utc::now().to_rfc3339(),
                        worker_id_for_heartbeat,
                        rpc_error
                    );

                    match CoordinatorServiceClient::connect(heartbeat_coordinator_addr.clone()).await {
                        Ok(new_client) => {
                            heartbeat_client = new_client;
                            println!( // Added timestamp
                                "[{}] Successfully reconnected heartbeat client for worker {}.",
                                chrono::Utc::now().to_rfc3339(),
                                worker_id_for_heartbeat
                            );

                            let immediate_heartbeat_info = HeartbeatInfo {
                                worker_id: worker_id_for_heartbeat.clone(),
                                timestamp: chrono::Utc::now().timestamp(),
                            };
                            if let Err(immediate_send_err_status) =
                                heartbeat_client.send_heartbeat(Request::new(immediate_heartbeat_info)).await
                            {
                                let immediate_send_rpc_err =
                                    WorkerError::RpcError(immediate_send_err_status);
                                eprintln!( // Added timestamp
                                    "[{}] Failed to send immediate heartbeat for worker {} after reconnect: {}",
                                    chrono::Utc::now().to_rfc3339(),
                                    worker_id_for_heartbeat,
                                    immediate_send_rpc_err
                                );
                            } else {
                                println!( // Added timestamp
                                    "[{}] Successfully sent immediate heartbeat for worker {} after reconnect.",
                                    chrono::Utc::now().to_rfc3339(),
                                    worker_id_for_heartbeat
                                );
                            }
                        }
                        Err(reconnect_err_original) => {
                            let conn_err = WorkerError::ClientConnection(reconnect_err_original);
                            eprintln!( // Added timestamp
                                "[{}] Failed to reconnect heartbeat client for worker {}. Error: {}. Will retry on next cycle.",
                                chrono::Utc::now().to_rfc3339(),
                                worker_id_for_heartbeat,
                                conn_err
                            );
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
