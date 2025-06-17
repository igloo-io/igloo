use arrow_flight::{
    flight_service_server::FlightService, Action, ActionType, Criteria, Empty, FlightData,
    FlightDescriptor, FlightInfo, GetFlightInfoRequest, HandshakeRequest, HandshakeResponse,
    PutResult, SchemaResult, Ticket,
};
use futures::Stream;
use igloo_api::IglooFlightSqlService;
use std::pin::Pin;
use tonic::{Request, Response, Status, Streaming};

pub struct CoordinatorFlightSqlService {
    inner: IglooFlightSqlService,
}

impl CoordinatorFlightSqlService {
    pub fn new(inner: IglooFlightSqlService) -> Self {
        Self { inner }
    }
}

#[tonic::async_trait]
impl FlightService for CoordinatorFlightSqlService {
    type HandshakeStream = Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send + Sync + 'static>>;
    type ListFlightsStream = Pin<Box<dyn Stream<Item = Result<FlightInfo, Status>> + Send + Sync + 'static>>;
    type GetFlightInfoStream = Pin<Box<dyn Stream<Item = Result<FlightInfo, Status>> + Send + Sync + 'static>>;
    type DoGetStream = Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + Sync + 'static>>;
    type DoPutStream = Pin<Box<dyn Stream<Item = Result<PutResult, Status>> + Send + Sync + 'static>>;
    type DoActionStream = Pin<Box<dyn Stream<Item = Result<arrow_flight::Result, Status>> + Send + Sync + 'static>>;
    type ListActionsStream = Pin<Box<dyn Stream<Item = Result<ActionType, Status>> + Send + Sync + 'static>>;
    type DoExchangeStream = Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + Sync + 'static>>;

    async fn handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        println!("CoordinatorFlightSqlService: handshake called");
        self.inner.handshake(request).await
    }

    async fn list_flights(
        &self,
        request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        println!("CoordinatorFlightSqlService: list_flights called");
        self.inner.list_flights(request).await
    }

    async fn get_flight_info(
        &self,
        request: Request<GetFlightInfoRequest>,
    ) -> Result<Response<Self::GetFlightInfoStream>, Status> {
        println!("CoordinatorFlightSqlService: get_flight_info stream called");
        self.inner.get_flight_info(request).await
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        println!("CoordinatorFlightSqlService: get_schema called");
        self.inner.get_schema(request).await
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        println!("CoordinatorFlightSqlService: do_get called");
        self.inner.do_get(request).await
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        println!("CoordinatorFlightSqlService: do_put called");
        self.inner.do_put(request).await
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        println!("CoordinatorFlightSqlService: do_action called");
        self.inner.do_action(request).await
    }

    async fn list_actions(
        &self,
        request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        println!("CoordinatorFlightSqlService: list_actions called");
        self.inner.list_actions(request).await
    }

    async fn do_exchange(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        println!("CoordinatorFlightSqlService: do_exchange called");
        self.inner.do_exchange(request).await
    }
}
