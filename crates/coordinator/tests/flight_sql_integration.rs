use arrow_array::RecordBatch;
use arrow_flight::{
    flight_service_client::FlightServiceClient, // For do_get
    flight_sql_client::FlightSqlServiceClient,  // For high-level SQL methods
    utils::flight_data_to_arrow_batch,          // Utility to convert FlightData
    FlightData,
    HandshakeRequest,
};
use arrow_schema::{DataType, Field, Schema};
use igloo_api::new_flight_sql_service; // To start the server
use std::net::SocketAddr;
use std::sync::Arc;
use tokio_stream::StreamExt; // For collecting stream results
use tonic::transport::Server; // For running the server // For Arc<Schema>

#[tokio::test]
async fn test_flight_sql_handshake_and_get_catalogs() {
    // 1. Setup and start the server
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap(); // Use port 0 for a random available port
    let server = new_flight_sql_service(); // Get the FlightServiceServer

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    let actual_addr = listener.local_addr().unwrap(); // Get the actual address the OS assigned

    println!("Test server listening on {}", actual_addr);

    tokio::spawn(async move {
        Server::builder()
            .add_service(server)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .expect("Test server failed to start");
    });

    // Brief pause to ensure the server is up. In a real-world scenario,
    // a more robust readiness check might be needed.
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // 2. Connect the client
    let client_url = format!("http://{}", actual_addr);
    let mut client = FlightSqlServiceClient::connect(client_url)
        .await
        .expect("Failed to connect to Flight SQL server");

    // 3. Perform Handshake
    let handshake_request =
        HandshakeRequest { protocol_version: "1.0.0".to_string(), payload: b"hello".to_vec() };
    let request_stream = tokio_stream::once(handshake_request);
    let mut response_stream = client
        .handshake(tonic::Request::new(request_stream))
        .await
        .expect("Handshake failed")
        .into_inner();

    let handshake_response = response_stream
        .next()
        .await
        .expect("No handshake response received")
        .expect("Error in handshake response");

    assert_eq!(handshake_response.protocol_version, "1.0.0");
    assert_eq!(handshake_response.payload, b"hello"); // Asserting the echo
    println!("Handshake successful with payload: {:?}", handshake_response.payload);

    // 4. Perform GetCatalogs
    // The FlightSqlServiceClient's get_catalogs method internally handles calling
    // GetFlightInfo(CommandGetCatalogs) and then DoGet(ticket).
    // However, the current high-level `get_catalogs` client method might not directly
    // align with how we've set up the server to *only* respond to a specific ticket.
    // The problem description for the service implies `do_get` expects a ticket that
    // *implicitly* refers to `CommandGetCatalogs` because `get_flight_info` would have set it up.
    // For this test, let's simulate the `GetFlightInfo` part by creating a ticket
    // that our server's `do_get` will recognize (even though it's a simple implementation).
    // In a full implementation, `get_flight_info` would return a specific ticket for `CommandGetCatalogs`.
    // Our `do_get` currently doesn't inspect the ticket bytes, it just serves the catalogs.
    // So, any valid `Ticket` would work for the current server `do_get` implementation.

    // First, call GetFlightInfo to get a ticket for GetCatalogs.
    // The current server `get_flight_info` is unimplemented.
    // So, we can't directly test the `get_catalogs` high-level client function yet
    // if it relies on a specific command in the FlightInfo request.
    // Instead, we will directly call `do_get` with an empty ticket, as our `do_get`
    // implementation currently doesn't parse the ticket.

    let ticket_bytes = b"get_catalogs_ticket".to_vec(); // Dummy ticket, server doesn't check it yet
    let ticket = arrow_flight::Ticket { ticket: ticket_bytes.into() };

    let mut doGetStream =
        client.do_get(ticket).await.expect("do_get for GetCatalogs failed").into_inner();

    // First FlightData: Schema
    let schema_flight_data = doGetStream
        .next()
        .await
        .expect("Stream did not return schema")
        .expect("Error in schema FlightData");

    let (decoded_schema, _ipc_schema) =
        arrow_flight::utils::try_schema_from_flight_data(&schema_flight_data)
            .expect("Failed to decode schema from FlightData");

    let expected_schema =
        Arc::new(Schema::new(vec![Field::new("catalog_name", DataType::Utf8, false)]));
    assert_eq!(Arc::new(decoded_schema), expected_schema, "Schema did not match");
    println!("Schema received and validated successfully.");

    // Second FlightData: RecordBatch (empty)
    let batch_flight_data = doGetStream
        .next()
        .await
        .expect("Stream did not return batch")
        .expect("Error in batch FlightData");

    // There should be no dictionary batches for this simple schema/batch
    let batches = flight_data_to_arrow_batch(
        &batch_flight_data,
        expected_schema.clone(),
        &[],
        &Default::default(),
    )
    .expect("Failed to decode RecordBatch from FlightData");

    assert_eq!(batches.len(), 1, "Expected one RecordBatch");
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 0, "RecordBatch should be empty");
    assert_eq!(batch.schema(), expected_schema, "RecordBatch schema did not match");
    println!("Empty RecordBatch received and validated successfully.");

    // Ensure the stream is exhausted
    assert!(doGetStream.next().await.is_none(), "Stream should be exhausted");
    println!("do_get stream exhausted as expected.");
}
