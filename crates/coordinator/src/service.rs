use chrono::Utc;
use igloo_api::igloo::{
    coordinator_service_server::CoordinatorService, ExecuteQueryRequest, ExecuteQueryResponse,
    HeartbeatInfo, HeartbeatResponse, RegistrationAck, WorkerInfo,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

use datafusion::prelude::SessionContext; // Changed path
use datafusion_sql::parser::DFParser;
use datafusion_sql::planner::SqlToRel;

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
        let planner = SqlToRel::new(&ctx); // Using &ctx directly

        // Parse the SQL string into AST an Statement
        match DFParser::parse_sql(&sql) {
            Ok(mut statements) => {
                if statements.len() != 1 {
                    return Err(Status::invalid_argument("Expected exactly one SQL statement"));
                }
                // DFParser returns a VecDeque<Statement>, but statement_to_plan expects a single Statement
                // We've already checked that there's exactly one.
                let statement = statements.pop_front().unwrap();

                // Convert the Statement to a LogicalPlan
                match planner.statement_to_plan(statement) {
                    Ok(logical_plan) => {
                        let plan_str = format!("{:?}", logical_plan);
                        println!("Generated LogicalPlan: {}", plan_str);
                        // For now, return the Debug string of the plan
                        Ok(Response::new(ExecuteQueryResponse { plan: plan_str }))
                    }
                    Err(e) => {
                        eprintln!("Error planning SQL: {:?}", e);
                        Err(Status::invalid_argument(format!("Error planning SQL: {}", e)))
                    }
                }
            }
            Err(e) => {
                eprintln!("Error parsing SQL with DFParser: {:?}", e);
                Err(Status::invalid_argument(format!("Error parsing SQL: {}", e)))
            }
        }
    }
}
