use chrono::Utc;
use igloo_api::igloo::{
    coordinator_service_server::CoordinatorService, HeartbeatInfo, HeartbeatResponse,
    RegistrationAck, WorkerInfo,
};
use igloo_api::arrow::flight::sql::{
    ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest, Any, CommandGetCatalogs,
    CommandStatementQuery, ProstAnyExt, StatementQueryTicket, TicketStatementQuery,
};
use igloo_api::arrow::flight::Ticket;
use igloo_api::{FlightSqlService, FlightService}; // Corrected import
use arrow_flight::utils::flight_data_from_arrow_batch;
use arrow_flight::{
    HandshakeRequest, HandshakeResponse, FlightInfo, FlightData, FlightDescriptor, SchemaResult, Action, ActionType, Criteria, Empty, PutResult, PollInfo
};
use arrow_schema::{Schema, Field, DataType};
use futures::Stream;
use std::pin::Pin;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

#[derive(Debug, Clone)]
pub struct WorkerState {
    pub last_seen: i64,
}

pub type ClusterState = Arc<Mutex<HashMap<String, WorkerState>>>;

pub struct MyCoordinatorService {
    pub cluster: ClusterState,
}

// Define a new struct for FlightSqlService implementation
pub struct FlightSqlServiceImpl {}

#[tonic::async_trait]
impl FlightSqlService for FlightSqlServiceImpl {
    async fn get_flight_info_sql(
        &self,
        query: CommandStatementQuery,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        if query.is::<CommandGetCatalogs>() {
            // Respond with an empty catalog schema
            let schema = Schema::empty();
            let flight_info = FlightInfo {
                schema: prost::bytes::Bytes::from(Vec::<u8>::new()), // Empty schema
                flight_descriptor: None,
                endpoint: vec![],
                total_records: 0,
                total_bytes: 0,
                ordered: false, // Data is not ordered
            };
            Ok(Response::new(flight_info))
        } else {
            Err(Status::unimplemented("Only CommandGetCatalogs is supported"))
        }
    }

    async fn do_get_sql(
        &self,
        _ticket: TicketStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status>
    where
        Self: FlightService,
    {
        Err(Status::unimplemented("do_get_sql is not implemented"))
    }
}

// Implement FlightService for FlightSqlServiceImpl to satisfy trait bounds
#[tonic::async_trait]
impl FlightService for FlightSqlServiceImpl {
    type HandshakeStream =
        Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send + Sync + 'static>>;
    type ListFlightsStream =
        Pin<Box<dyn Stream<Item = Result<FlightInfo, Status>> + Send + Sync + 'static>>;
    type DoGetStream = Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + Sync + 'static>>;
    type DoPutStream = Pin<Box<dyn Stream<Item = Result<PutResult, Status>> + Send + Sync + 'static>>;
    type DoExchangeStream =
        Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + Sync + 'static>>;
    type DoActionStream =
        Pin<Box<dyn Stream<Item = Result<arrow_flight::Result, Status>> + Send + Sync + 'static>>;
    type ListActionsStream =
        Pin<Box<dyn Stream<Item = Result<ActionType, Status>> + Send + Sync + 'static>>;

    async fn handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        let mut stream = request.into_inner();
        let (tx, rx) = mpsc::channel(1); // Buffer size of 1

        tokio::spawn(async move {
            if let Some(Ok(handshake_request)) = stream.message().await.unwrap_or_default() {
                // Respond with a simple HandshakeResponse
                let response = HandshakeResponse {
                    protocol_version: "1.0".to_string(), // Example protocol version
                    payload: handshake_request.payload, // Echo back the payload
                };
                tx.send(Ok(response)).await.unwrap_or_default();
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx)) as Self::HandshakeStream))
    }

    // Implement other FlightService methods as unimplemented
    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights is not implemented"))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let flight_descriptor = request.get_ref();
        if flight_descriptor.r#type == arrow_flight::FlightDescriptorType::Cmd as i32 {
            // Try to decode the specific SQL command from flight_descriptor.cmd
            // This requires knowing the expected type or using ProstAnyExt::unpack
            let any_cmd = prost_types::Any::decode(flight_descriptor.cmd.clone())
                .map_err(|e| Status::invalid_argument(format!("Failed to decode Any: {}", e)))?;

            // Example: Directly creating CommandStatementQuery for get_flight_info_sql
            // In a real scenario, you might match on any_cmd.type_url
            let command_statement_query = if any_cmd.is::<CommandGetCatalogs>() {
                CommandStatementQuery {
                    query: String::new(), // Or extract from Any if it was wrapped differently
                    transaction_id: None, // Or extract if available
                }
            } else {
                // Handle other SQL command types or return unimplemented
                return Err(Status::unimplemented(format!(
                    "Unsupported SQL command type_url: {}",
                    any_cmd.type_url
                )));
            };

            // Call the specific SQL handler
            self.get_flight_info_sql(command_statement_query, request).await
        } else {
            Err(Status::unimplemented(
                "get_flight_info is only implemented for Cmd type descriptors (SQL)",
            ))
        }
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema is not implemented"))
    }

    async fn do_get(
        &self,
        _request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        // This will be handled by do_get_sql for SQL queries
        Err(Status::unimplemented("do_get is not implemented for non-SQL, use do_get_sql"))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_put is not implemented"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange is not implemented"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action is not implemented"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions is not implemented"))
    }
    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented(
            "poll_flight_info is not implemented",
        ))
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
