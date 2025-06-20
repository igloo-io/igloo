use arrow_flight::{
    flight_service_server::FlightService, Action, ActionType, Criteria, Empty, FlightData,
    FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse, PutResult, SchemaResult,
    Ticket, PollInfo
};
use arrow_array::{Int32Array, RecordBatch};
use arrow_flight::utils;
use arrow_schema::{DataType, Field, Schema};
use futures::Stream;
use std::pin::Pin;
use std::sync::Arc; // For Arc<Schema>
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

#[derive(Clone)]
pub struct BasicFlightServiceImpl {}

impl BasicFlightServiceImpl {
    pub fn new() -> Self {
        Self {}
    }
}

#[tonic::async_trait]
impl FlightService for BasicFlightServiceImpl {
    type HandshakeStream =
        Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send + Sync + 'static>>;
    type ListFlightsStream =
        Pin<Box<dyn Stream<Item = Result<FlightInfo, Status>> + Send + Sync + 'static>>;
    type DoGetStream =
        Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + Sync + 'static>>;
    type DoPutStream =
        Pin<Box<dyn Stream<Item = Result<PutResult, Status>> + Send + Sync + 'static>>;
    type DoExchangeStream =
        Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + Sync + 'static>>;
    type DoActionStream =
        Pin<Box<dyn Stream<Item = Result<arrow_flight::Result, Status>> + Send + Sync + 'static>>;
    type ListActionsStream =
        Pin<Box<dyn Stream<Item = Result<ActionType, Status>> + Send + Sync + 'static>>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info"))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>, // Ignoring descriptor for now
    ) -> Result<Response<SchemaResult>, Status> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "result",
            DataType::Int32,
            false,
        )]));

        let schema_result = utils::schema_to_schema_result(schema.as_ref(), &arrow_flight::IpcMessage(vec![])) // Second arg is options, can be default
            .map_err(|e| Status::internal(format!("Failed to convert schema to SchemaResult: {}", e)))?;

        Ok(Response::new(schema_result))
    }

    async fn do_get(
        &self,
        _request: Request<Ticket>, // Ignoring ticket for now
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "result",
            DataType::Int32,
            false,
        )]));
        let array = Arc::new(Int32Array::from(vec![1]));
        let batch = RecordBatch::try_new(schema.clone(), vec![array])
            .map_err(|e| Status::internal(format!("Failed to create RecordBatch: {}", e)))?;

        let (tx, rx) = mpsc::channel(2); // Channel for FlightData

        // Spawn a task to send FlightData
        tokio::spawn(async move {
            let flight_data_vec = match utils::batches_to_flight_data(schema.as_ref(), vec![batch]) {
                Ok(data) => data,
                Err(e) => {
                    let _ = tx.send(Err(Status::internal(format!("Failed to convert batch to FlightData: {}", e)))).await;
                    return;
                }
            };

            for flight_data in flight_data_vec {
                if tx.send(Ok(flight_data)).await.is_err() {
                    // Receiver dropped, stop sending
                    break;
                }
            }
        });

        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx)) as Self::DoGetStream
        ))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_put"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions"))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info"))
    }
}
