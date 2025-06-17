// #![allow(clippy::doc_overindented_list_items)]
// TODO: API crate for gRPC and Arrow Flight definitions

// Re-export the generated proto code
pub mod igloo {
    include!(concat!(env!("OUT_DIR"), "/igloo.rs")); // Defines FlightService trait

    use crate::arrow::flight::protocol::{FlightData, FlightInfo, Ticket};
    use flight_service_server::FlightService; // Corrected import
    use tokio_stream::wrappers::ReceiverStream;

    #[tonic::async_trait]
    impl FlightService for super::IglooFlightSqlService {
        type DoGetStream = ReceiverStream<Result<FlightData, tonic::Status>>;

        async fn get_flight_info(
            &self,
            _request: tonic::Request<FlightInfo>,
        ) -> Result<tonic::Response<FlightInfo>, tonic::Status> {
            Err(tonic::Status::unimplemented(
                "IglooFlightSqlService::get_flight_info is not implemented",
            ))
        }

        async fn do_get(
            &self,
            _request: tonic::Request<Ticket>,
        ) -> Result<tonic::Response<Self::DoGetStream>, tonic::Status> {
            Err(tonic::Status::unimplemented("IglooFlightSqlService::do_get is not implemented"))
        }
    }
}

pub mod arrow {
    pub mod flight {
        pub mod protocol {
            include!(concat!(env!("OUT_DIR"), "/arrow.flight.protocol.rs"));
        }
    }
}

use arrow_flight::{
    flight_service_server::FlightService, /*Action, ActionType, Criteria, Empty,*/
};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PutResult, SchemaAsIpc, SchemaResult, Ticket,
};
use datafusion::arrow::ipc::writer::IpcWriteOptions;
use futures::Stream;
use igloo_engine::QueryEngine;
use std::pin::Pin;
use std::sync::Arc;
use tonic::{Request, Response, Status, Streaming};

pub struct IglooFlightSqlService {
    engine: Arc<QueryEngine>,
}

impl IglooFlightSqlService {
    pub fn new(engine: QueryEngine) -> Self {
        Self { engine: Arc::new(engine) }
    }
}

#[tonic::async_trait]
impl FlightService for IglooFlightSqlService {
    type HandshakeStream =
        Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send + 'static>>;
    type ListFlightsStream =
        Pin<Box<dyn Stream<Item = Result<FlightInfo, Status>> + Send + 'static>>;
    type DoGetStream = Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>;
    type DoPutStream = Pin<Box<dyn Stream<Item = Result<PutResult, Status>> + Send + 'static>>;
    type DoExchangeStream =
        Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>;
    type DoActionStream =
        Pin<Box<dyn Stream<Item = Result<arrow_flight::Result, Status>> + Send + 'static>>;
    type ListActionsStream =
        Pin<Box<dyn Stream<Item = Result<ActionType, Status>> + Send + 'static>>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake is not yet implemented"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights is not yet implemented"))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();
        let cmd_bytes = descriptor.cmd;
        if cmd_bytes.is_empty() {
            return Err(Status::invalid_argument("No SQL command in FlightDescriptor"));
        }
        let sql = String::from_utf8(cmd_bytes.to_vec()).unwrap_or_default();
        let batches = self.engine.execute(&sql).await;
        let schema = batches.first().map(|b| b.schema()).ok_or(Status::not_found("No results"))?;
        let options = IpcWriteOptions::default();
        let schema_ipc = SchemaAsIpc::new(schema.as_ref(), &options);
        let flight_data = FlightData::from(schema_ipc);
        let schema_bytes = flight_data.data_header;
        let flight_info = FlightInfo { schema: schema_bytes, ..Default::default() };
        Ok(Response::new(flight_info))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema is not yet implemented"))
    }

    async fn do_get(
        &self,
        _request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        Err(Status::unimplemented("do_get is not yet implemented"))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_put is not yet implemented"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange is not yet implemented"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action is not yet implemented"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions is not yet implemented"))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<arrow_flight::PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info is not yet implemented"))
    }
}
