use chrono::Utc;
use igloo_api::igloo::{
    coordinator_service_server::CoordinatorService, HeartbeatInfo, HeartbeatResponse,
    RegistrationAck, WorkerInfo, ExecuteQueryRequest, ExecuteQueryResponse
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

use datafusion::prelude::*;
use datafusion_sql::sql_to_rel::SqlToRel;
use datafusion_common::datafusion_err;
use datafusion::execution::context::SessionContext;


#[derive(Debug, Clone)]
pub struct WorkerState {
    pub last_seen: i64,
}

pub type ClusterState = Arc<Mutex<HashMap<String, WorkerState>>>;

pub struct MyCoordinatorService {
    pub cluster: ClusterState,
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

    async fn execute_query(
        &self,
        request: Request<ExecuteQueryRequest>,
    ) -> Result<Response<ExecuteQueryResponse>, Status> {
        let sql = request.into_inner().sql;
        println!("Received SQL query: {}", sql);

        // Create a new DataFusion SessionContext
        let ctx = SessionContext::new();

        // Create a SqlToRel planner
        let planner = SqlToRel::new(&ctx.state()); // SqlToRel requires a SessionState reference

        // Parse the SQL into a LogicalPlan
        match planner.sql_to_plan(&sql) {
            Ok(logical_plan) => {
                let plan_str = format!("{:?}", logical_plan);
                println!("Generated LogicalPlan: {}", plan_str);
                // For now, return the Debug string of the plan
                Ok(Response::new(ExecuteQueryResponse { plan: plan_str }))
            }
            Err(e) => {
                eprintln!("Error parsing SQL: {:?}", e);
                Err(Status::invalid_argument(format!("Error parsing SQL: {}", e)))
            }
        }
    }
}
