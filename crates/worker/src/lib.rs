use bincode;
use igloo_api::igloo::{
    coordinator_service_client::CoordinatorServiceClient,
    worker_service_server::{WorkerService, WorkerServiceServer},
    DataForTaskRequest, DataForTaskResponse, HeartbeatInfo, TaskDefinition, TaskResult, WorkerInfo,
};
use std::error::Error;
use std::net::SocketAddr;
use tokio::sync::oneshot;
use tokio::time::{sleep, Duration};
use tonic::{transport::Server, Request, Response, Status}; // For Box<dyn Error>

// MyWorkerService struct and its implementation
#[derive(Debug)] // Added Debug for MyWorkerService
pub struct MyWorkerService {
    // Made public
    pub worker_id: String, // Made public
}

#[tonic::async_trait]
impl WorkerService for MyWorkerService {
    async fn execute_task(
        &self,
        request: Request<TaskDefinition>,
    ) -> Result<Response<TaskResult>, Status> {
        let task_def = request.into_inner();
        let task_id = task_def.task_id;
        let payload = task_def.payload;

        let deserialized_plan_string: String = match bincode::deserialize(&payload) {
            Ok(s) => s,
            Err(e) => {
                eprintln!(
                    "Worker {}: Failed to deserialize plan for task {}: {}",
                    self.worker_id, task_id, e
                );
                return Err(Status::internal("Failed to deserialize plan"));
            }
        };

        println!(
            "Worker {} executing task {} with plan fragment: {}",
            self.worker_id, task_id, deserialized_plan_string
        );

        println!(
            "Worker {} simulating execution for task {}",
            self.worker_id, task_id
        );
        sleep(Duration::from_secs(1)).await;

        println!("Worker {} finished task {}", self.worker_id, task_id);

        Ok(Response::new(TaskResult {
            task_id: task_id.clone(),
            result: vec![],
            success: true,
        }))
    }

    async fn get_data_for_task(
        &self,
        request: Request<DataForTaskRequest>,
    ) -> Result<Response<DataForTaskResponse>, Status> {
        println!(
            "Worker {} received GetDataForTask for task: {:?}",
            self.worker_id,
            request.get_ref().task_id
        );
        Ok(Response::new(DataForTaskResponse { data: vec![] }))
    }
}

// run_worker_server function
pub async fn run_worker_server(
    worker_id: String,
    worker_addr: SocketAddr,
    coordinator_addr: String,
    shutdown_signal: oneshot::Receiver<()>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    // Register with coordinator
    // It's important to handle connection errors here more gracefully for a robust worker.

    const MAX_REGISTRATION_RETRIES: usize = 5;
    const REGISTRATION_RETRY_DELAY_SECS: u64 = 2;
    let mut registration_retry_count = 0;

    let mut client = loop {
        match CoordinatorServiceClient::connect(coordinator_addr.clone()).await {
            Ok(c) => break c,
            Err(e) => {
                registration_retry_count += 1;
                eprintln!(
                    "Worker {}: Failed to connect to coordinator (attempt {}/{}): {}. Retrying in {}s...",
                    worker_id, registration_retry_count, MAX_REGISTRATION_RETRIES, e, REGISTRATION_RETRY_DELAY_SECS
                );
                if registration_retry_count >= MAX_REGISTRATION_RETRIES {
                    return Err(format!(
                        "Worker {} failed to connect to coordinator after {} attempts",
                        worker_id, MAX_REGISTRATION_RETRIES
                    )
                    .into());
                }
                sleep(Duration::from_secs(REGISTRATION_RETRY_DELAY_SECS)).await;
            }
        }
    };

    registration_retry_count = 0; // Reset for registration attempts
    loop {
        let info = WorkerInfo {
            id: worker_id.clone(),
            address: format!("{}", worker_addr),
        };
        match client.register_worker(info.clone()).await {
            Ok(_) => {
                println!(
                    "Worker {} registered successfully with coordinator at {}",
                    worker_id, coordinator_addr
                );
                break; // Successfully registered
            }
            Err(e) => {
                registration_retry_count += 1;
                eprintln!(
                    "Worker {} failed registration attempt {}/{}: {}. Retrying in {}s...",
                    worker_id, registration_retry_count, MAX_REGISTRATION_RETRIES, e, REGISTRATION_RETRY_DELAY_SECS
                );
                if registration_retry_count >= MAX_REGISTRATION_RETRIES {
                    return Err(format!(
                        "Worker {} failed to register after {} attempts",
                        worker_id, MAX_REGISTRATION_RETRIES
                    )
                    .into());
                }
                sleep(Duration::from_secs(REGISTRATION_RETRY_DELAY_SECS)).await;
            }
        }
    }

    // Spawn heartbeat task
            // Clone client for heartbeat task. Consider a more robust client management strategy for long-running apps.
            let mut heartbeat_client = client; // Use the same client, or .clone() if connect returns a clonable client wrapper
            let hb_worker_id = worker_id.clone();
            let _heartbeat_task = tokio::spawn(async move {
                loop {
                    let heartbeat = HeartbeatInfo {
                        worker_id: hb_worker_id.clone(),
                        timestamp: chrono::Utc::now().timestamp(),
                    };
                    if let Err(e) = heartbeat_client.send_heartbeat(heartbeat).await {
                        eprintln!("Worker {}: Failed to send heartbeat: {}", hb_worker_id, e);
                        // Consider attempting to reconnect or other recovery logic here
                        // For now, we just print and continue trying.
                        // If connection is permanently lost, this task might spin without success.
                        // A robust solution might involve trying to reconnect CoordinatorServiceClient.
                        // This could happen if the coordinator restarts.
                        // For simplicity, we assume send_heartbeat handles this by erroring out,
                        // and we just log. If the client becomes invalid, this loop might need
                        // to re-establish connection.
                        // Let's try to reconnect if send_heartbeat fails for a more robust heartbeat
                        match CoordinatorServiceClient::connect(coordinator_addr.clone()).await {
                            Ok(new_client) => heartbeat_client = new_client,
                            Err(reconnect_err) => {
                                eprintln!(
                                    "Worker {}: Failed to reconnect coordinator for heartbeat: {}",
                                    hb_worker_id, reconnect_err
                                );
                                // Sleep before retrying connection to avoid tight loop on persistent error
                                sleep(Duration::from_secs(5)).await;
                                continue; // Skip this heartbeat attempt
                            }
                        }
                    }
                    sleep(Duration::from_secs(5)).await;
                }
            });

            println!(
                "Worker {} gRPC server starting on {}...",
                worker_id, worker_addr
            );
            Server::builder()
                .add_service(WorkerServiceServer::new(MyWorkerService {
                    worker_id: worker_id.clone(),
                }))
                .serve_with_shutdown(worker_addr, async {
                    shutdown_signal.await.ok();
                    println!(
                        "Worker {} received shutdown signal, shutting down.",
                        worker_id
                    );
                })
                .await?;

            // heartbeat_task.abort(); // Abort heartbeat on shutdown
            Ok(())
        // This Err case for initial connect is now handled by the loop above
        // However, the client for heartbeat could also fail.
        // The heartbeat loop already has a reconnect attempt.
}
