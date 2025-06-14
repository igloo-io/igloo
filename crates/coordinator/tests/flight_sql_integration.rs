use igloo_coordinator::service::FlightSqlServiceImpl;
use igloo_api::arrow::flight::{
    flight_service_client::FlightServiceClient,
    flight_service_server::FlightServiceServer,
    sql::{CommandGetCatalogs, CommandStatementQuery},
    FlightDescriptor, HandshakeRequest,
};
use prost::Message; // For encoding CommandGetCatalogs
use tonic::transport::{Channel, Server};
use tokio::net::TcpListener;
use tokio_stream::iter; // For creating a stream for handshake

async fn setup_server() -> Result<(FlightServiceClient<Channel>, SocketAddr), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let service = FlightSqlServiceImpl {};

    tokio::spawn(async move {
        Server::builder()
            .add_service(FlightServiceServer::new(service))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
    });

    let channel = Channel::from_shared(format!("http://{}", addr))?
        .connect()
        .await?;
    let client = FlightServiceClient::new(channel);

    Ok((client, addr))
}


#[tokio::test]
async fn test_flight_sql_get_catalogs() -> Result<(), Box<dyn std::error::Error>> {
    let (mut client, _addr) = setup_server().await?;

    // 1. Perform Handshake
    let request_stream = iter(vec![Ok(HandshakeRequest {
        protocol_version: "1.0".to_string(),
        payload: b"test_payload".to_vec().into(),
    })]);

    let mut response_stream = client.handshake(request_stream).await?.into_inner();
    if let Some(response) = response_stream.message().await? {
        assert_eq!(response.protocol_version, "1.0");
        assert_eq!(response.payload, b"test_payload".to_vec().into());
    } else {
        panic!("Handshake response was empty");
    }

    // 2. Send CommandGetCatalogs
    let get_catalogs_cmd = CommandGetCatalogs {};
    let cmd = CommandStatementQuery {
        query: "".to_string(), // Not used by CommandGetCatalogs in our impl
        transaction_id: None,
    };
    let mut buf = Vec::new();
    cmd.encode(&mut buf)?; // Encode CommandStatementQuery

    // Prepare FlightDescriptor
    let mut any_buf = Vec::new();
    get_catalogs_cmd.encode(&mut any_buf)?;
    let any_msg = prost_types::Any {
        type_url: CommandGetCatalogs::get_type_url(),
        value: any_buf,
    };
    let mut cmd_bytes = Vec::new();
    any_msg.encode(&mut cmd_bytes)?;


    let request = FlightDescriptor {
        r#type: arrow_flight::FlightDescriptorType::Cmd.into(),
        cmd: cmd_bytes.into(), // CommandGetCatalogs encoded into Any, then into bytes
        path: vec![],
    };

    let flight_info_response = client.get_flight_info(request).await?;
    let flight_info = flight_info_response.into_inner();

    // 3. Assert the response
    // Expecting an empty schema as per FlightSqlServiceImpl implementation
    assert!(flight_info.schema.is_empty(), "Schema should be empty for GetCatalogs");
    assert_eq!(flight_info.total_records, 0);
    assert_eq!(flight_info.total_bytes, 0);
    assert!(flight_info.endpoint.is_empty(), "Endpoints should be empty for GetCatalogs");

    Ok(())
}
