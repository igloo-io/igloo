#![allow(clippy::doc_overindented_list_items)]
// TODO: API crate for gRPC and Arrow Flight definitions

// Re-export the generated proto code
pub mod igloo {
    include!(concat!(env!("OUT_DIR"), "/igloo.rs")); // Defines FlightService trait

    use crate::arrow::flight::protocol::{FlightData, FlightInfo, Ticket};
    use flight_service_server::FlightService; // Corrected import
    use tokio_stream::wrappers::ReceiverStream;

    pub struct IglooFlightSqlService;

    #[tonic::async_trait]
    impl FlightService for IglooFlightSqlService {
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
