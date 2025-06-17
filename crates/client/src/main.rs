use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{FlightDescriptor, Ticket};
use arrow_ipc::reader::StreamReader;
use futures::stream::TryStreamExt;
use tonic::transport::Channel;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("igloo-client starting up...");

    let addr = "http://127.0.0.1:50051";
    println!("Connecting to Flight SQL server at {}...", addr);

    let mut client = match FlightServiceClient::connect(addr).await {
        Ok(client) => client,
        Err(e) => {
            eprintln!("Failed to connect to Flight SQL server: {}", e);
            return Err(Box::new(e));
        }
    };

    println!("Successfully connected to Flight SQL server.");

    // 1. Create a FlightDescriptor for the query
    let query = "SELECT 1 + 1 AS two";
    println!("Preparing query: {}", query);
    let flight_descriptor = FlightDescriptor::new_cmd(query.to_string());

    // 2. Call get_flight_info and print the schema
    println!("Calling get_flight_info...");
    let flight_info = match client.get_flight_info(flight_descriptor.clone()).await {
        Ok(response) => response.into_inner(),
        Err(e) => {
            eprintln!("Failed to get flight info: {}", e);
            return Err(Box::new(e));
        }
    };
    println!("Successfully retrieved FlightInfo.");

    if let Some(schema_bytes) = flight_info.schema.as_ref() {
        match arrow_ipc::convert::schema_from_bytes(schema_bytes) {
            Ok(schema) => {
                println!("Schema: {:?}", schema);
            }
            Err(e) => {
                eprintln!("Failed to decode schema: {}", e);
            }
        }
    } else {
        println!("No schema information received in FlightInfo.");
    }


    // 3. Create a Ticket for the same query (assuming single endpoint from FlightInfo)
    if flight_info.endpoint.is_empty() {
        eprintln!("No endpoints found in FlightInfo. Cannot execute query.");
        return Err("No endpoints available".into());
    }

    // For simplicity, using the first endpoint's ticket.
    // A real client might need to iterate or choose based on location, etc.
    let ticket_bytes = flight_info.endpoint[0].ticket.clone().unwrap_or_else(|| {
        // If the ticket in the endpoint is None, create a new one from the descriptor.
        // This might be necessary if the server doesn't populate endpoint tickets
        // but expects the client to use the descriptor's information.
        println!("Endpoint ticket is None, creating a new ticket from descriptor for do_get");
        Ticket { ticket: flight_descriptor.cmd.into() }
    });


    // 4. Call do_get to execute the query
    println!("Calling do_get to execute the query...");
    let flight_data_stream_response = match client.do_get(ticket_bytes).await {
        Ok(response) => response.into_inner(),
        Err(e) => {
            eprintln!("Failed to execute query (do_get): {}", e);
            return Err(Box::new(e));
        }
    };
    println!("Successfully initiated do_get.");

    // 5. Process the FlightData stream, convert to RecordBatches, and print them
    let mut flight_data_stream = flight_data_stream_response.map_err(|e| {
        arrow_ipc::Error::ExternalError(Box::new(e))
    });

    // The first message should be a schema
    if let Some(flight_data) = flight_data_stream.try_next().await? {
        if flight_data.data_header.is_empty() {
             eprintln!("Received FlightData without a schema header as the first message.");
             return Err("Missing schema header in FlightData stream".into());
        }
        // Process schema if needed, though we already got it from get_flight_info
        // For now, we assume the schema from get_flight_info is sufficient.
        println!("Received initial FlightData (expected schema).");
    } else {
        eprintln!("Stream ended before receiving any FlightData.");
        return Err("No FlightData received".into());
    }

    // Use StreamReader to convert FlightData stream to RecordBatches
    // We need to collect all FlightData messages first because StreamReader expects something that implements BufRead.
    // This is a simplification; a more robust client might process this in chunks or use a different IPC reading approach.

    let mut flight_data_vec = Vec::new();
    while let Some(flight_data) = flight_data_stream.try_next().await? {
        flight_data_vec.push(flight_data);
    }

    if flight_data_vec.is_empty() {
        println!("No data records received after the initial schema message.");
    } else {
        println!("Processing {} FlightData messages for RecordBatches...", flight_data_vec.len());
        // We need a way to feed these FlightData messages into an arrow_ipc::reader.
        // StreamReader expects a Read + Seek source, which is not what we have directly from the stream.
        // A common approach is to reconstruct the IPC stream bytes.
        // For simplicity here, let's assume the server sends Arrow IPC file format,
        // and we can try to read them one by one if they are self-contained batches.
        // However, FlightData typically streams dictionary batches first, then record batches.

        // A more correct way to handle this with arrow-rs is to use `FlightRecordBatchStream`
        // but that requires the server to send data in a specific way (e.g. with dictionaries first).
        // For this example, we'll try to read each FlightData that contains a data_body as a separate batch.
        // This is a simplification and might not work for all Flight implementations.

        let schema_from_flight_info = if let Some(ref schema_bytes) = flight_info.schema {
            arrow_ipc::convert::schema_from_bytes(schema_bytes).ok()
        } else {
            None
        };

        if schema_from_flight_info.is_none() {
            eprintln!("Cannot process RecordBatches without a schema from FlightInfo.");
            return Err("Schema not available for RecordBatch processing".into());
        }
        let schema_ref = std::sync::Arc::new(schema_from_flight_info.unwrap());


        for (i, flight_data) in flight_data_vec.iter().enumerate() {
            if !flight_data.data_body.is_empty() {
                 // Create a StreamReader for each FlightData message.
                 // This assumes each message is a self-contained IPC message for a record batch.
                 // We need to prepend the schema to the data body to form a valid IPC message.
                 // This is still a bit of a hack. FlightData is often not 1:1 with RecordBatch IPC messages.

                // A truly robust solution often involves a custom stream adapter or using higher-level
                // utilities from arrow-flight if available for this pattern.
                // The `arrow_flight::utils::flight_data_to_arrow_batch` is what we'd ideally use,
                // but it requires the schema and dictionaries to be handled correctly across the stream.

                // Let's try to use `arrow_ipc::reader::read_record_batch` if possible,
                // but it needs a `Read` source.
                // The most straightforward way with current arrow-rs for basic cases:
                // If the server sends schema with *every* FlightData (which is unusual but possible),
                // or if we can assume a simple stream without complex dictionary handling.

                // Given the complexity, for this example, we'll just print the fact that we received data.
                // A full IPC stream reconstruction is more involved.
                println!("Received FlightData message {} with data_body of length: {}", i, flight_data.data_body.len());

                // Attempt to read if schema is available and data_header indicates a record batch
                // The data_header in FlightData is the IPC message header for that batch.
                if !flight_data.data_header.is_empty() {
                    let mut reader = std::io::Cursor::new(&flight_data.data_header);
                    match arrow_ipc::meta::MessageHeader::FlatBuffer == arrow_ipc::meta::fb::Message::variant(&mut reader) {
                        Ok(arrow_ipc::meta::fb::MessageRef { header: Some(arrow_ipc::meta::fb::HeaderRef::RecordBatch(_)), .. }) => {
                            // We have a record batch message. Now read it.
                            // We need to combine data_header and data_body for the reader.
                            let mut ipc_message_bytes = Vec::new();
                            ipc_message_bytes.extend_from_slice(&flight_data.data_header);
                            ipc_message_bytes.extend_from_slice(&flight_data.data_body);

                            let mut cursor = std::io::Cursor::new(ipc_message_bytes);
                            match StreamReader::try_new(&mut cursor, None) { // `None` for schema, as it's in the message
                                Ok(mut stream_reader) => {
                                    if let Some(batch_result) = stream_reader.next() {
                                        match batch_result {
                                            Ok(batch) => {
                                                println!("RecordBatch {}: {:?}", i, batch);
                                            }
                                            Err(e) => {
                                                eprintln!("Failed to read RecordBatch {}: {}", i, e);
                                            }
                                        }
                                    } else {
                                         println!("StreamReader for FlightData {} yielded no batch.", i);
                                    }
                                }
                                Err(e) => {
                                    eprintln!("Failed to create StreamReader for FlightData {}: {}", i, e);
                                }
                            }
                        }
                        Ok(_) => {
                             println!("FlightData {} is not a RecordBatch message.", i);
                        }
                        Err(e) => {
                            eprintln!("Failed to determine message type for FlightData {}: {}", i, e);
                        }
                    }
                } else {
                    println!("FlightData {} has no data_header, cannot interpret as RecordBatch directly.", i);
                }
            } else {
                println!("Received FlightData message {} without data_body (likely schema or dictionary).", i);
            }
        }
    }


    println!("igloo-client finished successfully.");
    Ok(())
}
