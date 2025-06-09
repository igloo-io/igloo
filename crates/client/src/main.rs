use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::sql::CommandStatementQuery;
use arrow_flight::utils::flight_data_to_arrow_batch;
use arrow_flight::Ticket;
use tokio_stream::StreamExt;

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

    // Call get_flight_info with the query.
    let flight_info = client
        .get_flight_info(arrow_flight::Criteria::default(), cmd.into())
        .await?
        .into_inner();
    println!("Received FlightInfo.");

    // Extract the ticket.
    let ticket_bytes = flight_info
        .endpoint[0]
        .ticket
        .as_ref()
        .expect("No ticket found in FlightInfo")
        .ticket
        .clone();
    let ticket = Ticket { ticket: ticket_bytes };
    println!("Extracted ticket.");

    // Call do_get with the extracted ticket.
    let mut flight_stream = client.do_get(ticket).await?.into_inner();
    println!("Initiated data retrieval stream (do_get).");

    // Collect the stream of FlightData.
    let mut flight_data_vec = Vec::new();
    while let Some(flight_data_res) = flight_stream.next().await {
        let flight_data = flight_data_res?;
        println!(
            "Received FlightData (dictionary: {}, record_batches: {}, app_metadata_len: {})",
            flight_data.flight_descriptor.is_some(), // Simplified check; actual dict batch is via type
            flight_data.data_body.len(), // This is a rough indicator; proper check is complex
            flight_data.app_metadata.len()
        );
        flight_data_vec.push(flight_data);
    }
    println!("Collected all FlightData. Total items: {}", flight_data_vec.len());

    // Iterate through the collected FlightData and convert to RecordBatches.
    if flight_data_vec.is_empty() {
        println!("No data received from the server.");
    } else {
        println!("Processing received FlightData into RecordBatches:");
        for flight_data in flight_data_vec {
            // Assuming there's at least one schema message before data messages.
            // A more robust implementation would handle dictionaries and schemas across multiple FlightData messages.
            let schema = arrow_flight::SchemaAsIpc::try_from(&flight_data)?
                .ipc_message
                .ok_or("Expected schema in FlightData")?;

            let dictionaries_by_id = std::collections::HashMap::new(); // Assuming no dictionaries for simplicity

            match flight_data_to_arrow_batch(&flight_data, schema.into(), &dictionaries_by_id) {
                Ok(record_batch) => {
                    println!("Converted FlightData to RecordBatch:");
                    arrow::util::pretty::print_batches(&[record_batch])?;
                }
                Err(e) => {
                    eprintln!("Error converting FlightData to RecordBatch: {}", e);
                    // Optionally print raw FlightData for debugging
                    // eprintln!("Problematic FlightData: {:?}", flight_data);
                }
            }
        }
    }

    Ok(())
}
