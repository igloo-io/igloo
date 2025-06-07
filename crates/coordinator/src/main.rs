use chrono::Utc;

mod planner;
mod stage;

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

#[derive(Debug, Clone)]
struct WorkerState {
    last_seen: i64,
}

type ClusterState = Arc<Mutex<HashMap<String, WorkerState>>>;

struct MyCoordinatorService {
    cluster: ClusterState,
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
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cluster: ClusterState = Arc::new(Mutex::new(HashMap::new()));
    let cluster2 = cluster.clone();
    // Prune dead workers every 10 seconds
    tokio::spawn(async move {
        loop {
            let mut cluster = cluster2.lock().await;
            let now = Utc::now().timestamp();
            cluster.retain(|_, w| now - w.last_seen < 30);
            sleep(Duration::from_secs(10)).await;
        }
    });
    let addr: SocketAddr = "127.0.0.1:50051".parse()?;
    let svc = MyCoordinatorService { cluster };
    println!("Coordinator listening on {}", addr);
    Server::builder()
        .add_service(CoordinatorServiceServer::new(svc))
        .serve(addr)
        .await?;
    Ok(())
}
