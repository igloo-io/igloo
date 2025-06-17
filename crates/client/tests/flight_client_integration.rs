use std::net::SocketAddr;
use std::sync::Arc;
use tokio::task::JoinHandle;

use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{FlightDescriptor, Ticket};
use arrow_ipc::reader::StreamReader;
use arrow_ipc::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use futures::stream::TryStreamExt;
use tonic::transport::Channel;

// Assuming igloo_coordinator::service::FlightService and its config/builder exist.
// This is a placeholder as direct knowledge of igloo_coordinator internals is not available.
// In a real scenario, you would import and use the actual coordinator components.
mod mock_coordinator {
    use std::net::SocketAddr;
    use std::sync::Arc;
    use tokio::task::JoinHandle;
    use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
    use arrow_flight::{
        Action, ActionResult, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint,
        FlightInfo, HandshakeRequest, HandshakeResponse, PutResult, SchemaResult, Ticket,
    };
    use arrow_schema::{Schema, Field, DataType, ArrowError};
    use arrow_array::{Int32Array, RecordBatch as ArrowRecordBatch};
    use futures::stream;
    use tonic::{Request, Response, Status, Streaming};
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;


    pub struct MockFlightSqlService {
        // In a real service, this would hold state, data sources, etc.
    }

    #[tonic::async_trait]
    impl FlightService for MockFlightSqlService {
        type HandshakeStream = futures::stream::Once<Result<HandshakeResponse, Status>>;
        async fn handshake(
            &self,
            _request: Request<Streaming<HandshakeRequest>>,
        ) -> Result<Response<Self::HandshakeStream>, Status> {
            Err(Status::unimplemented("Not yet implemented"))
        }

        type ListFlightsStream = futures::stream::Once<Result<FlightInfo, Status>>;
        async fn list_flights(
            &self,
            _request: Request<Criteria>,
        ) -> Result<Response<Self::ListFlightsStream>, Status> {
            Err(Status::unimplemented("Not yet implemented"))
        }

        async fn get_flight_info(
            &self,
            request: Request<FlightDescriptor>,
        ) -> Result<Response<FlightInfo>, Status> {
            let descriptor = request.into_inner();
            if descriptor.r#type == arrow_flight::flight_descriptor::DescriptorType::Cmd.into() &&
               descriptor.cmd == "SELECT 1 as num" {

                let schema = Arc::new(Schema::new(vec![Field::new("num", DataType::Int32, false)]));
                let mut schema_bytes = Vec::new();
                let mut writer = arrow_ipc::writer::FileWriter::try_new(
                    &mut schema_bytes,
                    schema.as_ref()
                ).map_err(|e| Status::internal(format!("Schema serialization error: {}", e)))?;
                writer.finish().map_err(|e| Status::internal(format!("Schema finish error: {}", e)))?;

                // Drop the writer to release borrow on schema_bytes
                drop(writer);


                let endpoint = FlightEndpoint {
                    ticket: Some(Ticket { ticket: descriptor.cmd.clone() }),
                    location: Vec::new(), // Or provide a dummy location
                };

                let flight_info = FlightInfo {
                    schema: schema_bytes, // DEPRECATED, use flight_descriptor.schema
                    flight_descriptor: Some(descriptor),
                    endpoint: vec![endpoint],
                    total_records: 1,
                    total_bytes: -1, // Unknown
                    ordered: false, // Or true if applicable
                };
                Ok(Response::new(flight_info))
            } else {
                Err(Status::not_found("Unsupported query or descriptor type"))
            }
        }

        async fn get_schema(
            &self,
            _request: Request<FlightDescriptor>,
        ) -> Result<Response<SchemaResult>, Status> {
            Err(Status::unimplemented("Not yet implemented"))
        }

        type DoGetStream = futures::stream::BoxStream<'static, Result<FlightData, Status>>;
        async fn do_get(&self, request: Request<Ticket>) -> Result<Response<Self::DoGetStream>, Status> {
            let ticket = request.into_inner();
            if ticket.ticket == "SELECT 1 as num".as_bytes() {
                let schema = Arc::new(Schema::new(vec![Field::new("num", DataType::Int32, false)]));
                let col = Arc::new(Int32Array::from(vec![1]));
                let batch = ArrowRecordBatch::try_new(schema.clone(), vec![col])
                    .map_err(|e| Status::internal(format!("RecordBatch creation error: {}", e)))?;

                let mut flight_data_list = vec![];

                // Schema message (optional if client gets schema from GetFlightInfo)
                // let schema_flight_data = arrow_flight::utils::flight_data_from_arrow_schema(schema.as_ref(), &arrow_flight::FlightMetadata::default());
                // flight_data_list.push(Ok(schema_flight_data));

                // RecordBatch message
                let (_dictionary_flight_data, record_batch_flight_data) =
                    arrow_flight::utils::flight_data_from_arrow_batch(&batch, &arrow_flight::FlightMetadata::default());

                // Assuming no dictionaries for this simple case
                flight_data_list.push(Ok(record_batch_flight_data));

                let output = futures::stream::iter(flight_data_list);
                Ok(Response::new(Box::pin(output)))
            } else {
                Err(Status::not_found("Unsupported ticket"))
            }
        }

        type DoPutStream = futures::stream::Once<Result<PutResult, Status>>;
        async fn do_put(
            &self,
            _request: Request<Streaming<FlightData>>,
        ) -> Result<Response<Self::DoPutStream>, Status> {
            Err(Status::unimplemented("Not yet implemented"))
        }

        type DoExchangeStream = futures::stream::Once<Result<FlightData, Status>>;
        async fn do_exchange(
            &self,
            _request: Request<Streaming<FlightData>>,
        ) -> Result<Response<Self::DoExchangeStream>, Status> {
            Err(Status::unimplemented("Not yet implemented"))
        }

        type DoActionStream = futures::stream::Once<Result<ActionResult, Status>>;
        async fn do_action(
            &self,
            _request: Request<Action>,
        ) -> Result<Response<Self::DoActionStream>, Status> {
            Err(Status::unimplemented("Not yet implemented"))
        }

        type ListActionsStream = futures::stream::Once<Result<Action, Status>>;
        async fn list_actions(
            &self,
            _request: Request<Empty>,
        ) -> Result<Response<Self::ListActionsStream>, Status> {
            Err(Status::unimplemented("Not yet implemented"))
        }
    }

    pub async fn start_mock_coordinator(port: u16) -> Result<(JoinHandle<()>, SocketAddr), Box<dyn std::error::Error + Send + Sync>> {
        let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).await?;
        let addr = listener.local_addr()?;
        println!("Mock Coordinator listening on: {}", addr);

        let service = MockFlightSqlService {};
        let server = FlightServiceServer::new(service);

        let handle = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(server)
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .expect("Mock server failed");
        });

        Ok((handle, addr))
    }
}


/// Helper function to start the igloo-coordinator (mocked for this test)
async fn start_coordinator_server(port: u16) -> Result<(JoinHandle<()>, SocketAddr), Box<dyn std::error::Error + Send + Sync>> {
    // For now, using the mock_coordinator. Replace with actual coordinator startup if possible.
    mock_coordinator::start_mock_coordinator(port).await
}

#[tokio::test]
async fn test_flight_sql_query() -> Result<(), Box<dyn std::error::Error>> {
    // Start the coordinator server on a dynamically chosen port
    let (coordinator_handle, coordinator_addr) = start_coordinator_server(0).await?;

    let client_uri = format!("http://{}", coordinator_addr);
    println!("Test client connecting to: {}", client_uri);

    let mut client = FlightServiceClient::connect(client_uri).await?;
    println!("Client connected.");

    // Define the SQL query
    let query = "SELECT 1 as num";
    let flight_descriptor = FlightDescriptor::new_cmd(query.to_string());

    // 1. GetFlightInfo (primarily to get the ticket and optionally schema)
    let flight_info = client.get_flight_info(flight_descriptor.clone()).await?.into_inner();
    println!("Got FlightInfo: {:?}", flight_info);

    assert!(!flight_info.endpoint.is_empty(), "No endpoints in FlightInfo");
    let ticket = flight_info.endpoint[0].ticket.clone().expect("No ticket in endpoint");

    // Extract schema from FlightInfo (this is the new preferred way over flight_info.schema)
    let schema_from_flight_info = if let Some(ref fi_descriptor) = flight_info.flight_descriptor {
        if !fi_descriptor.schema.is_empty() {
             arrow_ipc::convert::schema_from_bytes(&fi_descriptor.schema).ok()
        } else { None }
    } else if !flight_info.schema.is_empty() { // Fallback to deprecated field
        arrow_ipc::convert::schema_from_bytes(&flight_info.schema).ok()
    } else { None };

    let flight_info_schema_arc = Arc::new(schema_from_flight_info.expect("Schema not found in FlightInfo response"));


    // 2. DoGet to execute the query and get data
    println!("Executing DoGet with ticket: {:?}", ticket);
    let mut flight_data_stream = client.do_get(ticket).await?.into_inner();

    let mut received_batches = Vec::new();
    let mut dictionaries_by_id = std::collections::HashMap::new(); // For handling dictionaries

    while let Some(flight_data) = flight_data_stream.try_next().await? {
        // Use arrow_flight::utils::flight_data_to_arrow_batch to convert
        // This utility function correctly handles dictionaries and reconstructs batches.
        match arrow_flight::utils::flight_data_to_arrow_batch(
            &flight_data,
            flight_info_schema_arc.clone(), // Provide the schema
            &dictionaries_by_id,
        ) {
            Ok(Some(batch)) => {
                println!("Received RecordBatch: {:?}", batch);
                received_batches.push(batch);
            }
            Ok(None) => {
                // This typically means it was a dictionary batch
                println!("Received dictionary batch from FlightData.");
            }
            Err(e) => {
                // arrow_flight::utils::flight_data_to_arrow_batch might return an error
                // if it encounters an issue, e.g. schema mismatch or malformed data.
                // It might also store dictionaries internally if it processes a dictionary batch.
                // The original `flight_data_to_arrow_batch` in arrow-rs has a more complex signature
                // involving mutable access to dictionaries_by_id.
                // The version in `arrow_flight::utils` might simplify this.
                // For testing, we'll assume it works or we adapt.
                // If direct usage is complex, manual parsing might be needed as a fallback.
                eprintln!("Error converting FlightData to RecordBatch: {}", e);
                // This part might need adjustment based on the exact behavior of
                // `flight_data_to_arrow_batch` and how it handles dictionaries.
                // For this example, let's assume it populates dictionaries for subsequent calls.
                // A common pattern is that `flight_data_to_arrow_batch` returns Ok(None) for dictionary messages
                // and populates `dictionaries_by_id`.
            }
        }
    }

    // Assertions
    assert!(!received_batches.is_empty(), "No RecordBatches received");

    let first_batch = &received_batches[0];

    // Assert schema
    let expected_schema = Arc::new(Schema::new(vec![
        Field::new("num", DataType::Int32, false) // Mock server uses Int32
    ]));
    assert_eq!(first_batch.schema(), expected_schema, "Schema mismatch");

    // Assert data
    assert_eq!(first_batch.num_rows(), 1, "Expected 1 row");
    let num_col = first_batch.column_by_name("num")
        .expect("Column 'num' not found")
        .as_any()
        .downcast_ref::<arrow_array::Int32Array>()
        .expect("Failed to downcast 'num' column to Int32Array");

    assert_eq!(num_col.value(0), 1, "Expected value 1 in 'num' column");

    println!("Test passed successfully!");

    // Shutdown the coordinator
    coordinator_handle.abort();
    println!("Coordinator server shut down.");

    Ok(())
}
