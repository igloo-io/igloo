// All imports from main.rs, potentially with adjustments if some are main-specific
// use bincode; // Removed as per clippy suggestion
use chrono::Utc;
use igloo_api::igloo::{
    coordinator_service_server::{CoordinatorService, CoordinatorServiceServer},
    worker_service_client::WorkerServiceClient,
    HeartbeatInfo, HeartbeatResponse, QueryRequest, QueryResponse, QueryStatusRequest,
    QueryStatusResponse, RegistrationAck, TaskDefinition, WorkerInfo,
};
use std::collections::HashMap;
use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex, MutexGuard};
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};
use tonic::{transport::Server, Request, Response, Status};

// Struct definitions (WorkerState, QueryInfo)
#[derive(Debug, Clone)]
pub struct WorkerState {
    pub address: String,
    pub last_seen: i64,
}

#[derive(Debug, Clone)]
pub struct QueryInfo {
    pub sql: String,
    pub status: String,
    pub submitted_at: i64,
    pub physical_plan: Option<Vec<u8>>,
    pub planned_at: Option<i64>,
    pub tasks: Option<Vec<TaskDefinition>>,
    pub distributed_plan_created_at: Option<i64>,
    pub task_statuses: Option<HashMap<String, String>>,
    pub dispatched_at: Option<i64>,
    pub finished_at: Option<i64>, // Added field for finished timestamp
}

// Type aliases (ClusterState, QueryState)
pub type ClusterState = Arc<Mutex<HashMap<String, WorkerState>>>;
pub type QueryState = Arc<Mutex<HashMap<String, QueryInfo>>>;

// create_distributed_plan function
// This function does not need to be public for the integration test's current scope,
// but if other parts of the library needed it, it could be.
// For now, keeping it crate-local (no `pub` before `fn`).
fn create_distributed_plan(query_id: &str, _physical_plan: &[u8]) -> Vec<TaskDefinition> {
    let num_tasks = 2;
    let mut tasks = Vec::new();
    for i in 0..num_tasks {
        let task_id = format!("{}_task_{}", query_id, i);
        let payload_string = format!("dummy_plan_fragment_for_task_{}", i);
        let payload = bincode::serialize(&payload_string).unwrap_or_default();
        tasks.push(TaskDefinition { task_id, payload });
    }
    println!(
        "Generated {} tasks for query {} (simulated).",
        tasks.len(),
        query_id
    );
    tasks
}

// MyCoordinatorService struct definition
// Needs to be pub if run_server is pub and uses it, which it does.
pub struct MyCoordinatorService {
    cluster: ClusterState,
    queries: QueryState,
}

// schedule_and_dispatch_tasks function
// Also crate-local for now, as it's an internal detail of the service implementation.
async fn schedule_and_dispatch_tasks(
    query_id: String,
    tasks_to_dispatch: Vec<TaskDefinition>,
    cluster_state: ClusterState,
    queries_state: QueryState,
) {
    let workers = {
        let cluster_lock: MutexGuard<HashMap<String, WorkerState>> = cluster_state.lock().await;
        cluster_lock
            .iter()
            .map(|(id, state)| (id.clone(), state.address.clone()))
            .collect::<Vec<(String, String)>>()
    };

    if workers.is_empty() {
        eprintln!(
            "Query {}: No workers available for task dispatch.",
            query_id
        );
        let mut queries_lock = queries_state.lock().await;
        if let Some(qi) = queries_lock.get_mut(&query_id) {
            qi.status = "DispatchFailed_NoWorkers".to_string();
        }
        return;
    }

    let num_workers = workers.len();

    for (task_idx, task) in tasks_to_dispatch.into_iter().enumerate() {
        let (worker_id, worker_address) = workers[task_idx % num_workers].clone();

        let full_worker_address = format!("http://{}", worker_address);
        let current_task_id = task.task_id.clone();
        let q_state_clone = queries_state.clone();
        let query_id_clone_for_task = query_id.clone();
        let worker_id_clone_for_task = worker_id.clone();
        let full_worker_address_clone_for_task = full_worker_address.clone();

        let _handle: JoinHandle<()> = tokio::spawn(async move {
            let task_status_update;
            match WorkerServiceClient::connect(full_worker_address_clone_for_task.clone()).await {
                Ok(mut client) => {
                    println!(
                        "Query {}: Dispatching task {} to worker {} at {}",
                        query_id_clone_for_task,
                        current_task_id,
                        worker_id_clone_for_task,
                        full_worker_address_clone_for_task
                    );
                    match client.execute_task(Request::new(task)).await {
                        Ok(response_wrapper) => {
                            let task_result = response_wrapper.into_inner();
                            if task_result.success {
                                println!(
                                    "Query {}: Task {} Succeeded on worker {}",
                                    query_id_clone_for_task,
                                    current_task_id,
                                    worker_id_clone_for_task
                                );
                                task_status_update = "Succeeded".to_string();
                            } else {
                                eprintln!(
                                    "Query {}: Task {} Failed on worker {} (worker reported failure)",
                                    query_id_clone_for_task, current_task_id, worker_id_clone_for_task
                                );
                                task_status_update = "Failed_Execution".to_string();
                            }
                        }
                        Err(e) => {
                            eprintln!(
                                "Query {}: RPC failed for task {} on worker {}: {:?}",
                                query_id_clone_for_task,
                                current_task_id,
                                worker_id_clone_for_task,
                                e
                            );
                            task_status_update = format!("Failed_RpcError_{}", e.code());
                        }
                    }
                }
                Err(e) => {
                    eprintln!(
                        "Query {}: Failed to connect to worker {} for task {}: {:?}",
                        query_id_clone_for_task, worker_id_clone_for_task, current_task_id, e
                    );
                    task_status_update = "Failed_Connect".to_string();
                }
            }

            // Update task status and check for overall query completion
            let mut queries_lock = q_state_clone.lock().await;
            if let Some(qi) = queries_lock.get_mut(&query_id_clone_for_task) {
                if let Some(ts) = qi.task_statuses.as_mut() {
                    ts.insert(current_task_id.clone(), task_status_update);
                }

                // Check if all tasks are completed and query is not already finished
                if qi.status != "Finished" {
                    let all_succeeded = qi.tasks.as_ref().is_some_and(|tasks| {
                        tasks.iter().all(|t| {
                            qi.task_statuses.as_ref().is_some_and(|s| {
                                s.get(&t.task_id)
                                    .is_some_and(|status| status == "Succeeded")
                            })
                        })
                    });

                    if all_succeeded {
                        println!(
                            "Query {}: All tasks succeeded. Marking query as Finished.",
                            query_id_clone_for_task
                        );
                        qi.status = "Finished".to_string();
                        qi.finished_at = Some(Utc::now().timestamp());
                    } else {
                        let any_failed = qi.tasks.as_ref().is_some_and(|tasks| {
                            tasks.iter().any(|t| {
                                qi.task_statuses.as_ref().is_some_and(|s| {
                                    s.get(&t.task_id)
                                        .is_some_and(|status| status.starts_with("Failed"))
                                })
                            })
                        });
                        if any_failed
                            && !qi.status.starts_with("Failed")
                            && !qi.status.starts_with("DispatchFailed")
                        {
                            println!(
                                "Query {}: At least one task failed. Marking query as Failed.",
                                query_id_clone_for_task
                            );
                            qi.status = "Failed".to_string();
                            // qi.finished_at = Some(Utc::now().timestamp()); // Optionally set finished_at on failure
                        }
                    }
                }
            }
        });
    }
}

// impl CoordinatorService for MyCoordinatorService
#[tonic::async_trait]
impl CoordinatorService for MyCoordinatorService {
    async fn register_worker(
        &self,
        request: Request<WorkerInfo>,
    ) -> Result<Response<RegistrationAck>, Status> {
        let info = request.into_inner();
        let mut cluster = self.cluster.lock().await;
        cluster.insert(
            info.id.clone(),
            WorkerState {
                address: info.address.clone(),
                last_seen: Utc::now().timestamp(),
            },
        );
        println!("Registered worker: {} at {}", info.id, info.address);
        Ok(Response::new(RegistrationAck {
            message: "Registered".to_string(),
        }))
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

    async fn submit_query(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<QueryResponse>, Status> {
        let req = request.into_inner();
        let query_id = req.query_id;
        let sql = req.sql;

        println!("Received query submission: {}, SQL: {}", query_id, sql);

        let submitted_at = Utc::now().timestamp();
        let initial_query_info = QueryInfo {
            sql: sql.clone(),
            status: "Submitted".to_string(),
            submitted_at,
            physical_plan: None,
            planned_at: None,
            tasks: None,
            distributed_plan_created_at: None,
            task_statuses: None,
            dispatched_at: None,
            finished_at: None, // Initialize new field
        };

        {
            let mut queries_lock = self.queries.lock().await;
            queries_lock.insert(query_id.clone(), initial_query_info);
        }

        let dummy_physical_plan_bytes: Vec<u8> =
            bincode::serialize("dummy_physical_plan").unwrap_or_default();

        {
            let mut queries_lock = self.queries.lock().await;
            if let Some(qi) = queries_lock.get_mut(&query_id) {
                qi.physical_plan = Some(dummy_physical_plan_bytes.clone());
                qi.status = "Planned".to_string();
                qi.planned_at = Some(Utc::now().timestamp());
                println!("Query {} physical planning complete (simulated).", query_id);
            } else {
                eprintln!(
                    "Error: Query {} not found for physical planning update.",
                    query_id
                );
                return Err(Status::internal(format!(
                    "Query {} disappeared before planning",
                    query_id
                )));
            }
        }

        let tasks_for_dispatch = {
            let mut queries_lock = self.queries.lock().await;
            if let Some(qi) = queries_lock.get_mut(&query_id) {
                let generated_tasks =
                    create_distributed_plan(&query_id, &dummy_physical_plan_bytes);
                qi.tasks = Some(generated_tasks.clone());
                qi.status = "TasksGenerated".to_string();
                qi.distributed_plan_created_at = Some(Utc::now().timestamp());
                println!("Query {} task generation complete (simulated).", query_id);
                generated_tasks
            } else {
                eprintln!(
                    "Error: Query {} not found for task generation update.",
                    query_id
                );
                return Err(Status::internal(format!(
                    "Query {} disappeared before task generation",
                    query_id
                )));
            }
        };

        let q_id_clone = query_id.clone();
        let cluster_clone = self.cluster.clone();
        let queries_clone = self.queries.clone();

        tokio::spawn(async move {
            {
                let mut queries_lock = queries_clone.lock().await;
                if let Some(qi) = queries_lock.get_mut(&q_id_clone) {
                    qi.status = "Dispatching".to_string();
                    qi.dispatched_at = Some(Utc::now().timestamp());
                    qi.task_statuses = Some(HashMap::new());
                    println!("Query {}: Status set to Dispatching.", q_id_clone);
                } else {
                    eprintln!(
                        "Query {}: Not found when trying to set status to Dispatching.",
                        q_id_clone
                    );
                    return;
                }
            }
            schedule_and_dispatch_tasks(
                q_id_clone,
                tasks_for_dispatch,
                cluster_clone,
                queries_clone,
            )
            .await;
        });

        Ok(Response::new(QueryResponse {
            query_id,
            status: "Submitted".to_string(),
        }))
    }

    async fn get_query_status(
        &self,
        request: Request<QueryStatusRequest>,
    ) -> Result<Response<QueryStatusResponse>, Status> {
        let req = request.into_inner();
        let query_id = req.query_id;

        println!("Received GetQueryStatus request for query_id: {}", query_id);

        let queries_lock = self.queries.lock().await;
        if let Some(query_info) = queries_lock.get(&query_id) {
            let response = QueryStatusResponse {
                query_id: query_id.clone(),
                status: query_info.status.clone(),
                sql: query_info.sql.clone(),
                physical_plan_bytes: query_info.physical_plan.clone(),
                tasks: query_info.tasks.clone().unwrap_or_default(),
                submitted_at: Some(query_info.submitted_at),
                planned_at: query_info.planned_at,
                distributed_plan_created_at: query_info.distributed_plan_created_at,
                dispatched_at: query_info.dispatched_at,
                finished_at: query_info.finished_at,
                task_statuses: query_info.task_statuses.clone().unwrap_or_default(),
            };
            Ok(Response::new(response))
        } else {
            Err(Status::not_found(format!(
                "Query with ID {} not found",
                query_id
            )))
        }
    }
}

// run_server function
pub async fn run_server(
    addr: SocketAddr,
    cluster_state: ClusterState,
    query_state: QueryState,
    shutdown_signal: oneshot::Receiver<()>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let svc = MyCoordinatorService {
        cluster: cluster_state.clone(),
        queries: query_state.clone(),
    };

    let cluster_prune_state = cluster_state.clone();
    let prune_task = tokio::spawn(async move {
        loop {
            let mut cluster_lock = cluster_prune_state.lock().await;
            let now = Utc::now().timestamp();
            cluster_lock.retain(|_, w| now - w.last_seen < 30);
            drop(cluster_lock);
            sleep(Duration::from_secs(10)).await;
        }
    });

    println!("Coordinator listening on {}", addr);
    Server::builder()
        .add_service(CoordinatorServiceServer::new(svc))
        .serve_with_shutdown(addr, async {
            shutdown_signal.await.ok();
            println!("Coordinator received shutdown signal, shutting down.");
        })
        .await?;

    prune_task.abort();
    Ok(())
}
