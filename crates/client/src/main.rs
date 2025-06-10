use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::sql::CommandStatementQuery;
use arrow_flight::utils::flight_data_to_arrow_batch;
use arrow_flight::{FlightData, FlightDescriptor, FlightInfo, IpcMessage, Ticket, flight_descriptor}; // Added FlightDescriptor and flight_descriptor
use arrow::datatypes::Schema; // For Schema type - Reverted to arrow::datatypes
use std::sync::Arc; // For Arc<Schema>
use tokio_stream::StreamExt; // Ensure StreamExt is in scope
use prost::Message; // For encoding prost messages
use prost::bytes::Bytes; // For Bytes type - Changed path

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a FlightServiceClient and connect to the server.
    let mut client = FlightServiceClient::connect("http://localhost:50051").await?;
    println!("Connected to Flight SQL server.");

    // Create a CommandStatementQuery with the SQL query.
    let cmd = CommandStatementQuery {
        query: "SELECT * FROM mock_table".to_string(),
        transaction_id: None,
    };
    println!("Created SQL query: {}", cmd.query);

    // Encode CommandStatementQuery to bytes
    let mut cmd_bytes = Vec::new();
    cmd.encode(&mut cmd_bytes)?;

    // Create FlightDescriptor
    let descriptor = FlightDescriptor {
        r#type: flight_descriptor::DescriptorType::Cmd as i32,
        cmd: Bytes::from(cmd_bytes),
        path: vec![],
    };

    // Call get_flight_info with the query.
    let flight_info_response: FlightInfo = client
        .get_flight_info(descriptor)
        .await?
        .into_inner();
    println!("Received FlightInfo.");

    // Parse the schema from FlightInfo
    // flight_info.schema is Bytes, wrap it in IpcMessage
    let ipc_message = IpcMessage(flight_info_response.schema);
    let schema_arrow = Schema::try_from(ipc_message)?; // Convert IpcMessage to Schema

    let schema_ref: Arc<Schema> = Arc::new(schema_arrow);
    println!("Parsed schema from FlightInfo.");

    // Extract the ticket (assuming there's at least one endpoint).
    let ticket: Ticket = flight_info_response
        .endpoint
        .get(0)
        .and_then(|ep| ep.ticket.clone())
        .ok_or("No ticket found in FlightInfo")?;
    println!("Extracted ticket.");

    // Call do_get with the extracted ticket.
    let mut flight_stream = client.do_get(ticket).await?.into_inner();
    println!("Initiated data retrieval stream (do_get).");

    // Collect the stream of FlightData.
    let mut flight_data_vec: Vec<FlightData> = Vec::new();
    while let Some(flight_data_res) = flight_stream.next().await {
        let flight_data = flight_data_res?;
        println!(
            "Received FlightData (descriptor: {}, body_len: {}, app_metadata_len: {})",
            flight_data.flight_descriptor.is_some(),
            flight_data.data_body.len(),
            flight_data.app_metadata.len()
        );
        flight_data_vec.push(flight_data);
    }
    println!("Collected all FlightData. Total items: {}", flight_data_vec.len());

    // Iterate through the collected FlightData and convert to RecordBatches.
    if flight_data_vec.is_empty() {
        println!("No data received from the server.");
    } else {
        println!("Processing received FlightData into RecordBatches using schema from FlightInfo:");
        let dictionaries_by_id = std::collections::HashMap::new();

        for flight_data in flight_data_vec {
            match flight_data_to_arrow_batch(&flight_data, schema_ref.clone(), &dictionaries_by_id) {
                Ok(record_batch) => {
                    if record_batch.num_rows() > 0 {
                        println!("Converted FlightData to RecordBatch ({} rows):", record_batch.num_rows());
                        arrow::util::pretty::print_batches(&[record_batch])?;
                    } else {
                        println!("Converted FlightData to an empty RecordBatch (likely metadata or dictionary).");
                    }
                }
                Err(e) => {
                    eprintln!("Error converting FlightData to RecordBatch: {}", e);
                }
            }
        }
    }

    Ok(())
}
