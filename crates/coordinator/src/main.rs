use chrono::Utc;
mod config;
use config::Settings;
mod error;
use error::CoordinatorError;
use igloo_api::igloo::{
    coordinator_service_server::{CoordinatorService, CoordinatorServiceServer},
    HeartbeatInfo, HeartbeatResponse, RegistrationAck, WorkerInfo,
};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};
use tonic::{transport::Server, Request, Response, Status};

// ClusterState and WorkerState remain the same
// MyCoordinatorService struct and its impl CoordinatorService remain the same

#[derive(Debug, Clone)]
pub struct WorkerState { // Made public
    pub last_seen: i64, // Made public for potential test inspection through ClusterState
}

pub type ClusterState = Arc<Mutex<HashMap<String, WorkerState>>>; // Already effectively public

pub struct MyCoordinatorService { // Made public
    cluster: ClusterState,
}

pub async fn run_server(
    settings: Settings, // Use Settings directly from config module
    cluster_state: ClusterState,
) -> Result<(), CoordinatorError> {
    let addr = settings.server_address()?;

    let cluster_state_for_pruning = cluster_state.clone();
    let prune_interval_secs = settings.prune_interval_secs;
    let worker_timeout_secs = settings.worker_timeout_secs;

    tokio::spawn(async move {
        loop {
            let mut cluster = cluster_state_for_pruning.lock().await;
            let now = Utc::now().timestamp();
            cluster.retain(|worker_id, w| {
                if now - w.last_seen < worker_timeout_secs {
                    true
                } else {
                    println!("Pruning dead worker: {} due to timeout of {}s", worker_id, worker_timeout_secs);
                    false
                }
            });
            drop(cluster);
            sleep(Duration::from_secs(prune_interval_secs)).await;
        }
    });

    let svc = MyCoordinatorService { cluster: cluster_state };
    println!("Coordinator (test mode or actual) listening on {}", addr);
    Server::builder()
        .add_service(CoordinatorServiceServer::new(svc))
        .serve(addr)
        .await?;
    Ok(())
}

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
}

#[tokio::main]
async fn main() -> Result<(), CoordinatorError> {
    let settings = Settings::new()?;
    let cluster_state: ClusterState = Arc::new(Mutex::new(HashMap::new()));
    run_server(settings, cluster_state).await
}
