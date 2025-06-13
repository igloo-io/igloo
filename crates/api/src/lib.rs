#![allow(clippy::doc_overindented_list_items)]
// TODO: API crate for gRPC and Arrow Flight definitions

// Re-export the generated proto code
pub mod igloo {
    include!(concat!(env!("OUT_DIR"), "/igloo.rs")); // Defines FlightService trait
                                                     // flight_service_server is already included in igloo.rs
                                                     // use flight_service_server::FlightService;
}

pub mod arrow {
    pub mod flight {
        pub mod protocol {
            include!(concat!(env!("OUT_DIR"), "/arrow.flight.protocol.rs"));
            pub mod sql {
                // Add the sql module
                include!(concat!(env!("OUT_DIR"), "/arrow.flight.protocol.sql.rs"));
            }
        }
    }
}

use arrow_array::RecordBatch; // For creating RecordBatch
use arrow_flight::{
    flight_service_server::{FlightService, FlightServiceServer}, // Import FlightServiceServer
    utils::flight_data_from_arrow_batch, // Utility for FlightData from RecordBatch
    Action,
    ActionType,
    Criteria,
    Empty,
    FlightData,
    FlightDescriptor,
    FlightInfo,
    HandshakeRequest,
    HandshakeResponse,
    PutResult,
    SchemaAsIpc, // Import SchemaAsIpc
    SchemaResult,
    Ticket,
};
use arrow_schema::{DataType, Field, Schema}; // For schema definition
use futures::{stream, Stream, StreamExt}; // For stream manipulation
use std::pin::Pin;
use std::sync::Arc; // For Arc<Schema>
use tonic::{Request, Response, Status, Streaming};

// Define the service struct
pub struct FlightSqlService {}

#[tonic::async_trait]
impl FlightService for FlightSqlService {
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
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        let mut in_stream = request.into_inner();
        let output = async_stream::try_stream! {
            while let Some(request) = in_stream.next().await {
                let request = request?;
                // Echo back the payload or use a server-defined token
                // For simplicity, echoing the payload
                let response = HandshakeResponse {
                    payload: request.payload,
                    protocol_version: "1.0.0".to_string(), // Example protocol version
                };
                yield response;
            }
        };
        Ok(Response::new(Box::pin(output) as Self::HandshakeStream))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights is not yet implemented"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info is not yet implemented"))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema is not yet implemented"))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();
        // For this task, we assume the ticket is for CommandGetCatalogs.
        // In a real implementation, you would deserialize the ticket's cmd
        // and check if it's a CommandGetCatalogs.
        // e.g. using Any::unpack to CommandGetCatalogs from prost_types::Any
        // For now, we proceed directly to creating the GetCatalogs response.

        // Define the schema for GetCatalogs: a single non-nullable Utf8 field named "catalog_name".
        let schema = Arc::new(Schema::new(vec![Field::new("catalog_name", DataType::Utf8, false)]));

        // Create an empty RecordBatch conforming to the schema.
        // No rows, but the schema is defined.
        let batch = RecordBatch::new_empty(schema.clone());

        // Create FlightData message for the schema.
        let options = arrow_flight::ipc::writer::IpcWriteOptions::default();
        let schema_flight_data: FlightData = SchemaAsIpc::new(&schema, &options).into();

        // Create FlightData message for the empty RecordBatch.
        let (_dict_flight_data, batch_flight_data) = flight_data_from_arrow_batch(&batch, &options);
        // We expect _dict_flight_data to be empty here as we don't have dictionary columns.

        let output = stream::iter(vec![Ok(schema_flight_data), Ok(batch_flight_data)]);

        Ok(Response::new(Box::pin(output) as Self::DoGetStream))
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
}

// Helper function to create a new FlightSqlService and wrap it in FlightServiceServer
pub fn new_flight_sql_service() -> FlightServiceServer<FlightSqlService> {
    FlightServiceServer::new(FlightSqlService {})
}
