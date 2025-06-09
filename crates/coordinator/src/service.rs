use chrono::Utc;

use crate::scheduler::split_plan; // From the module created in the previous step
use futures::future::join_all;
use igloo_api::igloo::worker_service_client::WorkerServiceClient;
use igloo_api::igloo::{
    coordinator_service_server::CoordinatorService,
    HeartbeatInfo,
    HeartbeatResponse,
    RegistrationAck,
    TaskDefinition,
    // TaskStatus, // Removed as it's not directly used by coordinator service logic
    WorkerInfo,
};
use igloo_engine::physical_plan::PhysicalPlan; // Re-exported from engine
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct WorkerState {
    pub last_seen: i64,
}

pub type ClusterState = Arc<Mutex<HashMap<String, WorkerState>>>;

#[derive(Clone)] // Added Clone
pub struct MyCoordinatorService {
    pub cluster: ClusterState,
}

// Implementation block for MyCoordinatorService specific methods
impl MyCoordinatorService {
    async fn dispatch_plan_to_workers(&self, plan_to_execute: Arc<PhysicalPlan>) {
        let worker_addresses = [
            "http://127.0.0.1:50051", // Worker 1
            "http://127.0.0.1:50052", // Worker 2
            "http://127.0.0.1:50053", // Worker 3
        ]; // Changed to array as per clippy
        let num_workers = worker_addresses.len();

        println!("Original plan: {:?}", plan_to_execute);
        let partitioned_plans = split_plan(plan_to_execute, num_workers);
        println!("Split into {} partitioned plans.", partitioned_plans.len());

        if partitioned_plans.is_empty() {
            println!("No plans to dispatch after splitting.");
            return;
        }

        let mut dispatch_futures = Vec::new();

        for (i, p_plan) in partitioned_plans.iter().enumerate() {
            let worker_addr = worker_addresses[i % num_workers].to_string();
            let task_id = Uuid::new_v4().to_string();

            let serialized_plan = match bincode::serialize(p_plan.as_ref()) {
                Ok(bytes) => bytes,
                Err(e) => {
                    eprintln!("Failed to serialize plan for task {}: {}", task_id, e);
                    continue;
                }
            };

            let task_definition =
                TaskDefinition { task_id: task_id.clone(), payload: serialized_plan };

            println!(
                "Dispatching task {} to worker at {} with plan: {:?}",
                task_id, worker_addr, p_plan
            );

            let dispatch_future = async move {
                match WorkerServiceClient::connect(worker_addr.clone()).await {
                    Ok(mut client) => {
                        match client.execute_task(tonic::Request::new(task_definition)).await {
                            Ok(response) => {
                                println!(
                                    "Task {} successfully submitted to {}. Status: {}",
                                    task_id,
                                    worker_addr,
                                    response.into_inner().status
                                );
                                Ok((task_id.clone(), worker_addr, "SUCCESS".to_string()))
                            }
                            Err(e) => {
                                eprintln!(
                                    "Failed to execute task {} on {}: {:?}",
                                    task_id, worker_addr, e
                                );
                                Err((task_id.clone(), worker_addr, format!("RPC_ERROR: {:?}", e)))
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Failed to connect to worker {}: {:?}", worker_addr, e);
                        Err((task_id.clone(), worker_addr, format!("CONNECT_ERROR: {:?}", e)))
                    }
                }
            };
            dispatch_futures.push(dispatch_future);
        }

        let results = join_all(dispatch_futures).await;
        println!("Dispatch results: {:?}", results);
    }
}

#[tonic::async_trait]
impl CoordinatorService for MyCoordinatorService {
    async fn register_worker(
        &self,
        request: Request<WorkerInfo>,
    ) -> Result<Response<RegistrationAck>, Status> {
        let info = request.into_inner();
        let mut cluster_guard = self.cluster.lock().await;
        cluster_guard.insert(info.id.clone(), WorkerState { last_seen: Utc::now().timestamp() });
        println!(
            "Registered worker: {} at {}. Total workers: {}",
            info.id,
            info.address,
            cluster_guard.len()
        );

        if cluster_guard.len() == 3 {
            println!("Three workers registered. Initiating test plan dispatch...");
            let test_scan_plan = Arc::new(PhysicalPlan::Scan {
                table_name: "customer".to_string(),
                columns: vec!["c_custkey".to_string(), "c_name".to_string()],
                predicate: None,
                partition_id: None,
                total_partitions: None,
            });
            let self_clone = self.clone();
            tokio::spawn(async move {
                self_clone.dispatch_plan_to_workers(test_scan_plan).await;
            });
        }

        Ok(Response::new(RegistrationAck { message: "Registered".to_string() }))
    }
    async fn send_heartbeat(
        &self,
        request: Request<HeartbeatInfo>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let hb = request.into_inner();
        let mut cluster = self.cluster.lock().await;
        if let Some(worker) = cluster.get_mut(&hb.worker_id) {
            worker.last_seen = Utc::now().timestamp();
            println!("Heartbeat from worker: {}", hb.worker_id);
            Ok(Response::new(HeartbeatResponse { ok: true }))
        } else {
            Ok(Response::new(HeartbeatResponse { ok: false }))
        }
    }
}
