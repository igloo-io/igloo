use igloo_api::arrow_flight::{
    flight_service_server::FlightService, Action, ActionType, Criteria, Empty, FlightData,
    FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse, PutResult, SchemaResult,
    Ticket,
};
use futures::{Stream, StreamExt};
use std::pin::Pin;
use tokio_stream::wrappers::ReceiverStream; // Corrected import
use tonic::{Request, Response, Status, Streaming};

pub struct IglooflightSqlService {}

#[tonic::async_trait]
impl FlightService for IglooflightSqlService {
    type HandshakeStream =
        Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send + 'static>>;
    type ListFlightsStream =
        Pin<Box<dyn Stream<Item = Result<FlightInfo, Status>> + Send + 'static>>;
    type DoGetStream = Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>;
    type DoPutStream = Pin<Box<dyn Stream<Item = Result<PutResult, Status>> + Send + 'static>>;
    type DoExchangeStream =
        Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>;
    type DoActionStream =
        Pin<Box<dyn Stream<Item = Result<igloo_api::arrow_flight::Result, Status>> + Send + 'static>>;
    type ListActionsStream =
        Pin<Box<dyn Stream<Item = Result<ActionType, Status>> + Send + 'static>>;

    async fn handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        let mut stream = request.into_inner();
        let (tx, rx) = tokio::sync::mpsc::channel(1); // Buffer size of 1

        tokio::spawn(async move {
            while let Some(req_result) = futures::StreamExt::next(&mut stream).await {
                match req_result {
                    Ok(req) => {
                        // Respond with a simple handshake response
                        let response = HandshakeResponse {
                            protocol_version: req.protocol_version,
                            payload: req.payload, // Echo payload back
                        };
                        if tx.send(Ok(response)).await.is_err() {
                            // If send fails, it means the receiver is dropped.
                            // We can break the loop or log an error.
                            eprintln!("Receiver dropped, closing handshake stream");
                            break;
                        }
                    }
                    Err(e) => {
                        if tx.send(Err(e)).await.is_err() {
                            eprintln!("Receiver dropped, closing handshake stream after error");
                            break;
                        }
                    }
                }
            }
        });

        Ok(Response::new(Box::pin(
            ReceiverStream::new(rx), // Corrected usage
        )))
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
        // Assuming the ticket for GetCatalogs is a JSON string "GetCatalogs"
        // In a real scenario, you'd parse the ticket to determine the command
        if ticket.ticket == br#"{"command":"GetCatalogs"}"#.to_vec() {
            // For GetCatalogs, return an empty stream of FlightData
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            // Immediately close the sender for an empty response
            drop(tx);

            Ok(Response::new(Box::pin(
                ReceiverStream::new(rx), // Corrected usage
            )))
        } else {
            Err(Status::unimplemented(format!(
                "do_get for ticket {:?} is not yet implemented",
                ticket.ticket
            )))
        }
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
