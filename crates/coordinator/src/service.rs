use chrono::Utc;
use igloo_api::igloo::{
    coordinator_service_server::CoordinatorService, HeartbeatInfo, HeartbeatResponse,
    RegistrationAck, WorkerInfo,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

#[derive(Debug, Clone)]
pub struct WorkerState {
    pub last_seen: i64,
}

use crate::catalog::Catalog;

pub type ClusterState = Arc<Mutex<HashMap<String, WorkerState>>>;

pub struct MyCoordinatorService {
    pub cluster: ClusterState,
    pub catalog: Arc<Catalog>,
}

impl MyCoordinatorService {
    pub fn new() -> Self {
        Self {
            cluster: Arc::new(Mutex::new(HashMap::new())),
            catalog: Arc::new(Catalog::new()),
        }
    }
}

#[tonic::async_trait]
impl CoordinatorService for MyCoordinatorService {
    async fn register_worker(
        &self,
        request: Request<WorkerInfo>,
    ) -> Result<Response<RegistrationAck>, Status> {
        // TODO: Authentication/authorization stub
        // e.g., check request.metadata() for auth token
        // TODO: Add TLS support for gRPC in main.rs
        let info = request.into_inner();
        let mut cluster = self.cluster.lock().await;
        cluster.insert(info.id.clone(), WorkerState { last_seen: Utc::now().timestamp() });
        println!("Registered worker: {} at {}", info.id, info.address);
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
