use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use igloo_engine::QueryEngine;
use std::sync::Arc;

mod service;

use arrow_flight::flight_service_server::FlightServiceServer;
use igloo_api::IglooFlightSqlService;
use std::net::SocketAddr;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Instantiate the query engine
    let engine = QueryEngine::new();

    // 2. Register the CSV as a table
    let csv_path = "../connectors/filesystem/test_data.csv";
    let abs_csv_path = std::fs::canonicalize(csv_path)?;
    let abs_dir = abs_csv_path.parent().unwrap();
    let dir_url = format!("file://{}", abs_dir.display());
    let file_url = format!("file://{}", abs_csv_path.display());
    println!("Resolved CSV path: {}", abs_csv_path.display());
    let table_url = ListingTableUrl::parse(&dir_url)?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("col_a", DataType::Int64, false),
        Field::new("col_b", DataType::Utf8, false),
    ]));
    let format = Arc::new(CsvFormat::default().with_has_header(true));
    let options = ListingOptions::new(format).with_file_extension(".csv".to_string());
    let mut config =
        ListingTableConfig::new(table_url).with_schema(schema).with_listing_options(options);
    config.table_paths = vec![ListingTableUrl::parse(&file_url)?];
    let provider = ListingTable::try_new(config)?;
    engine.register_table("test_table", Arc::new(provider))?;

    // Start Arrow Flight SQL server
    let addr: SocketAddr = "127.0.0.1:50051".parse()?;
    let flight_service = IglooFlightSqlService::new(engine.clone());
    println!("Coordinator Flight SQL listening on {}", addr);
    Server::builder()
        .add_service(FlightServiceServer::new(flight_service))
        .serve_with_shutdown(addr, async {
            tokio::signal::ctrl_c().await.expect("failed to listen for event");
            println!("Shutting down coordinator gracefully...");
        })
        .await?;
    Ok(())
}
