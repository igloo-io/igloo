use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{FlightDescriptor, FlightInfo, Ticket};
use igloo_coordinator::launch_coordinator; // Assuming a helper function in main.rs or lib.rs
use std::net::SocketAddr;
use std::time::Duration;
use tokio::task::JoinHandle;
use tonic::transport::Channel;

async fn start_test_server() -> (JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>>, SocketAddr) {
    // Use a specific port for testing to simplify.
    let test_addr: SocketAddr = "127.0.0.1:50052".parse().unwrap();

    let server_handle = tokio::spawn(launch_coordinator(Some(test_addr)));
    // Give the server a moment to start and bind the port
    tokio::time::sleep(Duration::from_millis(500)).await;

    (server_handle, test_addr) // Return the handle and the address it's supposed to listen on
}

#[tokio::test]
async fn test_get_flight_info() {
    let (server_handle, addr) = start_test_server().await;

    // Connect to the server
    let channel = Channel::from_shared(format!("http://{}", addr))
        .unwrap()
        .connect_timeout(Duration::from_secs(5))
        .connect()
        .await
        .expect("Failed to connect to coordinator");
    let mut client = FlightServiceClient::new(channel);

    // Call get_flight_info
    let request = FlightDescriptor::new_cmd("SELECT 1".as_bytes().to_vec());
    let response = client.get_flight_info(request).await;

    assert!(response.is_ok(), "get_flight_info failed: {:?}", response.err());
    let flight_info = response.unwrap().into_inner();
    // Basic assertion: check that a FlightInfo object is returned.
    println!("Received FlightInfo: {:?}", flight_info);
    assert!(!flight_info.endpoint.is_empty(), "No endpoints in FlightInfo");

    // Cleanup: Abort the server task
    server_handle.abort();
}

#[tokio::test]
async fn test_do_get() {
    let (server_handle, addr) = start_test_server().await;

    // Connect to the server
    let channel = Channel::from_shared(format!("http://{}", addr))
        .unwrap()
        .connect_timeout(Duration::from_secs(5))
        .connect()
        .await
        .expect("Failed to connect to coordinator");
    let mut client = FlightServiceClient::new(channel);

    // First, call get_flight_info to get a ticket
    let request_info = FlightDescriptor::new_cmd("SELECT 1".as_bytes().to_vec());
    let flight_info_response = client.get_flight_info(request_info).await;
    assert!(flight_info_response.is_ok(), "get_flight_info for do_get failed");
    let flight_info = flight_info_response.unwrap().into_inner();
    assert!(!flight_info.endpoint.is_empty(), "No endpoints in FlightInfo for do_get");
    assert!(flight_info.endpoint[0].ticket.is_some(), "No ticket in FlightInfo endpoint for do_get");

    let ticket = flight_info.endpoint[0].ticket.clone().unwrap();

    // Call do_get
    let response_stream = client.do_get(ticket).await;

    assert!(response_stream.is_ok(), "do_get failed: {:?}", response_stream.err());
    let mut flight_data_stream = response_stream.unwrap().into_inner();

    // Basic assertion: try to receive at least one FlightData message
    let data = flight_data_stream.message().await;
    assert!(data.is_ok(), "Failed to receive message from do_get stream: {:?}", data.err());
    assert!(data.unwrap().is_some(), "do_get stream ended prematurely or was empty");

    println!("Received data from do_get stream");

    // Cleanup: Abort the server task
    server_handle.abort();
}
