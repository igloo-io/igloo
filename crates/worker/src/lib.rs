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
use tonic::{transport::Server, Request, Response, Status};
use chrono::Utc; // Added for Utc::now()

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
            let mut hb_client = client; // client is from successful registration
            let hb_worker_id = worker_id.clone();
            let coordinator_addr_for_hb = coordinator_addr.clone(); // Clone coordinator_addr string for the async block
            const HEARTBEAT_INTERVAL_SECS: u64 = 5;

            let _heartbeat_task = tokio::spawn(async move {
                let mut consecutive_heartbeat_failures = 0u32;
                loop {
                    let heartbeat = HeartbeatInfo {
                        worker_id: hb_worker_id.clone(),
                        timestamp: Utc::now().timestamp(),
                    };

                    match hb_client.send_heartbeat(heartbeat.clone()).await {
                        Ok(_) => {
                            if consecutive_heartbeat_failures > 0 {
                                println!(
                                    "Worker {}: Heartbeat successful to coordinator after {} failure(s).",
                                    hb_worker_id, consecutive_heartbeat_failures
                                );
                            }
                            consecutive_heartbeat_failures = 0; // Reset on success
                        }
                        Err(e) => {
                            consecutive_heartbeat_failures += 1;
                            eprintln!(
                                "Worker {}: Heartbeat send attempt {} failed: {}. Attempting to reconnect.",
                                hb_worker_id, consecutive_heartbeat_failures, e
                            );

                            let backoff_secs = std::cmp::min(60, 2u64.pow(consecutive_heartbeat_failures));
                            eprintln!(
                                "Worker {}: Waiting {}s before attempting to reconnect client for heartbeat.",
                                hb_worker_id, backoff_secs
                            );
                            sleep(Duration::from_secs(backoff_secs)).await;

                            match CoordinatorServiceClient::connect(coordinator_addr_for_hb.clone()).await {
                                Ok(new_client) => {
                                    println!(
                                        "Worker {}: Reconnected to coordinator successfully for heartbeat.",
                                        hb_worker_id
                                    );
                                    hb_client = new_client;
                                    // Do not reset consecutive_heartbeat_failures here.
                                    // Let the next successful send_heartbeat do it.
                                }
                                Err(reconnect_err) => {
                                    eprintln!(
                                        "Worker {}: Failed to reconnect to coordinator during heartbeat sequence: {}. Consecutive failures: {}",
                                        hb_worker_id, reconnect_err, consecutive_heartbeat_failures
                                    );
                                    // Will sleep for HEARTBEAT_INTERVAL_SECS then try again.
                                }
                            }
                        }
                    }
                    sleep(Duration::from_secs(HEARTBEAT_INTERVAL_SECS)).await;
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
