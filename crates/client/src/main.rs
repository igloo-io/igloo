use anyhow::Result;
use arrow::datatypes::{DataType, SchemaRef, TimeUnit, f16};
use arrow::ipc::reader::{read_schema, IpcReadOptions};
use arrow::record_batch::RecordBatch;
use arrow_flight::utils::flight_data_to_arrow_batch;
use arrow_flight::FlightData;
use igloo_api::arrow::flight::protocol::{FlightInfo, Ticket};
use igloo_api::igloo::{
    igloo_client_flight_service_client::IglooClientFlightServiceClient, CommandSql,
};
use prettytable::{format::Alignment, Cell, Row, Table}; // Added Alignment
use rustyline::error::ReadlineError;
use rustyline::Editor;
use std::io::Cursor;
use std::sync::Arc;
use arrow::array::*;
use tokio_stream::StreamExt;
use tonic::transport::Channel;
use clap::Parser; // Added clap

// Default coordinator address
const DEFAULT_COORDINATOR_ADDRESS: &str = "http://[::1]:50051";

/// Igloo SQL Client CLI
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct CliArgs {
    /// Address of the Igloo coordinator
    #[clap(short, long, value_parser)]
    coordinator_address: Option<String>,
}

// format_value function remains the same...
fn format_value(batch: &RecordBatch, col_idx: usize, row_idx: usize) -> String {
    let array = batch.column(col_idx);
    if array.is_null(row_idx) {
        return "NULL".to_string();
    }

    match array.data_type() {
        DataType::Null => "NULL".to_string(),
        DataType::Boolean => array.as_any().downcast_ref::<BooleanArray>().unwrap().value(row_idx).to_string(),
        DataType::Int8 => array.as_any().downcast_ref::<Int8Array>().unwrap().value(row_idx).to_string(),
        DataType::Int16 => array.as_any().downcast_ref::<Int16Array>().unwrap().value(row_idx).to_string(),
        DataType::Int32 => array.as_any().downcast_ref::<Int32Array>().unwrap().value(row_idx).to_string(),
        DataType::Int64 => array.as_any().downcast_ref::<Int64Array>().unwrap().value(row_idx).to_string(),
        DataType::UInt8 => array.as_any().downcast_ref::<UInt8Array>().unwrap().value(row_idx).to_string(),
        DataType::UInt16 => array.as_any().downcast_ref::<UInt16Array>().unwrap().value(row_idx).to_string(),
        DataType::UInt32 => array.as_any().downcast_ref::<UInt32Array>().unwrap().value(row_idx).to_string(),
        DataType::UInt64 => array.as_any().downcast_ref::<UInt64Array>().unwrap().value(row_idx).to_string(),
        DataType::Float16 => {
            let val_bits = array.as_any().downcast_ref::<Float16Array>().unwrap().value(row_idx);
            format!("F16({})", val_bits.to_bits())
        }
        DataType::Float32 => array.as_any().downcast_ref::<Float32Array>().unwrap().value(row_idx).to_string(),
        DataType::Float64 => array.as_any().downcast_ref::<Float64Array>().unwrap().value(row_idx).to_string(),
        DataType::Utf8 => array.as_any().downcast_ref::<StringArray>().unwrap().value(row_idx).to_string(),
        DataType::LargeUtf8 => array.as_any().downcast_ref::<LargeStringArray>().unwrap().value(row_idx).to_string(),
        DataType::Timestamp(unit, tz_opt) => {
            let tz_str = tz_opt.as_ref().map_or("", |tz| tz.as_ref());
            let val_str = match unit {
                 TimeUnit::Second => array.as_any().downcast_ref::<TimestampSecondArray>().unwrap().value_as_datetime(row_idx).map_or("invalid".to_string(), |dt| dt.to_string()),
                 TimeUnit::Millisecond => array.as_any().downcast_ref::<TimestampMillisecondArray>().unwrap().value_as_datetime(row_idx).map_or("invalid".to_string(), |dt| dt.to_string()),
                 TimeUnit::Microsecond => array.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap().value_as_datetime(row_idx).map_or("invalid".to_string(), |dt| dt.to_string()),
                 TimeUnit::Nanosecond => array.as_any().downcast_ref::<TimestampNanosecondArray>().unwrap().value_as_datetime(row_idx).map_or("invalid".to_string(), |dt| dt.to_string()),
            };
            format!("{} {}", val_str, tz_str)
        },
        DataType::Date32 => array.as_any().downcast_ref::<Date32Array>().unwrap().value_as_date(row_idx).map_or_else(|| "Invalid Date".to_string(), |d| d.to_string()),
        DataType::Date64 => array.as_any().downcast_ref::<Date64Array>().unwrap().value_as_datetime(row_idx).map_or_else(|| "Invalid DateTime".to_string(), |d| d.to_string()),
        DataType::Binary => format!("Binary ({} bytes)", array.as_any().downcast_ref::<BinaryArray>().unwrap().value(row_idx).len()),
        DataType::LargeBinary => format!("LargeBinary ({} bytes)", array.as_any().downcast_ref::<LargeBinaryArray>().unwrap().value(row_idx).len()),
        DataType::FixedSizeBinary(size) => format!("FixedSizeBinary({})", size),
        _ => format!("Unformatted: {:?}", array.data_type()),
    }
}

// execute_query function remains the same...
async fn execute_query(
    client: &mut IglooClientFlightServiceClient<Channel>,
    sql: &str,
) -> Result<()> {
    let command = CommandSql {
        query: sql.to_string(),
    };

    let request = tonic::Request::new(command);
    let flight_info_response: FlightInfo = match client.get_flight_info_sql(request).await {
        Ok(response) => response.into_inner(),
        Err(e) => {
            eprintln!("Error getting flight info: {}", e);
            return Err(e.into());
        }
    };

    let arrow_schema: SchemaRef = if !flight_info_response.schema.is_empty() {
        let mut cursor = Cursor::new(&flight_info_response.schema);
        match read_schema(&mut cursor, &IpcReadOptions::default()) {
            Ok(s) => Arc::new(s),
            Err(e) => {
                eprintln!(
                    "Failed to deserialize schema from FlightInfo: {}. Schema bytes length: {}",
                    e,
                    flight_info_response.schema.len()
                );
                return Err(anyhow::anyhow!("Schema deserialization failed: {}", e));
            }
        }
    } else {
        eprintln!("No schema found in FlightInfo response.");
        return Err(anyhow::anyhow!("Schema not provided in FlightInfo"));
    };

    let schema_for_conversion: SchemaRef = arrow_schema.clone();

    let ticket_bytes = if let Some(endpoint) = flight_info_response.endpoint.first() {
        if let Some(ticket_proto) = &endpoint.ticket {
            ticket_proto.ticket.clone()
        } else {
            eprintln!("No ticket found in the first endpoint.");
            return Ok(());
        }
    } else {
        eprintln!("No endpoints received from GetFlightInfoSql.");
        return Ok(());
    };

    let ticket = Ticket { ticket: ticket_bytes };

    let mut stream = match client.do_get(ticket).await {
        Ok(response) => response.into_inner(),
        Err(e) => {
            eprintln!("Error calling DoGet: {}", e);
            return Err(e.into());
        }
    };

    let mut table = Table::new();
    table.set_format(*prettytable::format::consts::FORMAT_BOX_CHARS);
    let header_cells: Vec<Cell> = arrow_schema
        .fields()
        .iter()
        .map(|field| Cell::new(field.name()).style_spec("bFcH"))
        .collect();
    table.set_titles(Row::new(header_cells));

    let mut record_batches_received = 0;
    let mut total_rows_processed = 0;
    let mut dictionaries_by_id = std::collections::HashMap::new();

    while let Some(flight_data_result) = stream.next().await {
        match flight_data_result {
            Ok(flight_data) => {
                match flight_data_to_arrow_batch(&flight_data, schema_for_conversion.clone(), &mut dictionaries_by_id) {
                    Ok(Some(batch)) => {
                        record_batches_received += 1;
                        if batch.num_rows() > 0 {
                            for row_idx in 0..batch.num_rows() {
                                let mut table_row_cells = Vec::new();
                                for col_idx in 0..batch.num_columns() {
                                    let cell_value = format_value(&batch, col_idx, row_idx);
                                    let data_type = batch.schema().field(col_idx).data_type();
                                    let mut cell = Cell::new(&cell_value);
                                    if data_type.is_numeric() {
                                        cell = cell.style_spec("r");
                                    } else {
                                        cell = cell.style_spec("l");
                                    }
                                    table_row_cells.push(cell);
                                }
                                table.add_row(Row::new(table_row_cells));
                            }
                            total_rows_processed += batch.num_rows();
                        }
                    }
                    Ok(None) => { /* Dictionary batch or empty */ }
                    Err(e) => {
                        eprintln!("  > Error converting FlightData to RecordBatch: {}", e);
                    }
                }
            }
            Err(e) => {
                eprintln!("Error receiving FlightData from stream: {}", e);
                break;
            }
        }
    }

    if total_rows_processed > 0 {
        table.printstd();
        println!("Query finished. Displayed {} rows from {} RecordBatch(es).", total_rows_processed, record_batches_received);
    } else if record_batches_received > 0 && total_rows_processed == 0 {
         println!("Query finished. Received {} RecordBatch(es), but they contained no rows.", record_batches_received);
    } else {
        println!("Query returned no data or no RecordBatches were successfully processed.");
    }
    Ok(())
}


#[tokio::main]
async fn main() -> Result<()> {
    let args = CliArgs::parse();
    let coordinator_address = args.coordinator_address.unwrap_or_else(|| DEFAULT_COORDINATOR_ADDRESS.to_string());

    println!("igloo-client starting up...");
    println!("Attempting to connect to coordinator at {}...", coordinator_address);

    let channel = match Channel::from_shared(coordinator_address.clone()) // Use from_shared for String
        .unwrap_or_else(|_| {
            panic!(
                "Invalid coordinator address format: {}",
                coordinator_address
            )
        })
        .connect()
        .await
    {
        Ok(ch) => ch,
        Err(e) => {
            eprintln!("Failed to connect to coordinator: {}", e);
            eprintln!(
                "Please ensure the Igloo coordinator is running at {}.",
                coordinator_address
            );
            return Err(e.into());
        }
    };
    let mut client = IglooClientFlightServiceClient::new(channel);
    println!("Connected to coordinator. Type SQL queries, 'exit', or 'quit'.");

    let mut rl = Editor::<()>::new()?;
    if rl.load_history("igloo_history.txt").is_err() {
        // No previous history found.
    }

    loop {
        let readline = rl.readline("igloo> ");
        match readline {
            Ok(line) => {
                let command = line.trim();
                if command.is_empty() {
                    continue;
                }
                rl.add_history_entry(command).unwrap_or_default();

                if command.eq_ignore_ascii_case("exit") || command.eq_ignore_ascii_case("quit") {
                    println!("Exiting igloo-client.");
                    break;
                }

                if let Err(e) = execute_query(&mut client, command).await {
                    eprintln!("Query execution error: {}", e);
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("CTRL-C");
            }
            Err(ReadlineError::Eof) => {
                println!("CTRL-D");
                break;
            }
            Err(err) => {
                eprintln!("Error reading line: {:?}", err);
                break;
            }
        }
    }

    if rl.save_history("igloo_history.txt").is_err() {
        // Failed to save history.
    }
    println!("igloo-client shut down.");
    Ok(())
}
