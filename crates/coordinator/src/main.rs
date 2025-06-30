use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::util::pretty::print_batches;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use igloo_engine::QueryEngine;
use std::path::Path;
use std::sync::Arc; // For path operations

mod service;

use crate::flight::BasicFlightServiceImpl; // Added
use arrow_flight::flight_service_server::FlightServiceServer;
use std::net::SocketAddr;
use tonic::transport::Server;

mod flight;
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Instantiate the query engine
    let engine = QueryEngine::new();

    // 2. Register the CSV as a table
    let csv_relative_path = "../connectors/filesystem/test_data.csv";
    let base_path = std::env::current_dir()?; // Assuming run from workspace root
    let csv_abs_path = base_path.join(Path::new(csv_relative_path));

    let table_path_url_str = format!("file://{}", csv_abs_path.display());
    let table_path = ListingTableUrl::parse(&table_path_url_str)?;

    println!("Attempting to register test_table with path: {}", csv_abs_path.display());

    let config = ListingTableConfig::new(table_path)
        .with_schema(Arc::new(Schema::new(vec![
            Field::new("col_a", DataType::Int64, false),
            Field::new("col_b", DataType::Utf8, false),
        ])))
        .with_listing_options(
            ListingOptions::new(Arc::new(CsvFormat::default().with_has_header(true)))
                .with_file_extension(".csv"),
        );

    let table_provider = Arc::new(ListingTable::try_new(config)?);
    engine.register_table("test_table", table_provider)?;
    println!("Registered test_table successfully.");

    // Register a Parquet file as a table
    let parquet_path = base_path.join("path/to/your_file.parquet");
    let parquet_url = ListingTableUrl::parse(format!("file://{}", parquet_path.display()))?;
    let parquet_config = ListingTableConfig::new(parquet_url).with_listing_options(
        ListingOptions::new(Arc::new(ParquetFormat::default())).with_file_extension(".parquet"),
    );
    let parquet_table = Arc::new(ListingTable::try_new(parquet_config)?);
    engine.register_table("parquet_table", parquet_table)?;

    // 3. Execute a simple query
    let sql = "SELECT col_a, col_b FROM test_table LIMIT 5;";
    println!("Executing SQL: {}", sql);
    let results = engine.execute(sql).await;

    // 4. Print results
    println!("Query Results:");
    match print_batches(&results) {
        Ok(_) => {} // Already prints to stdout
        Err(e) => eprintln!("Error printing batches: {}", e),
    }
    println!("Finished printing query results.");

    // Keep the existing Flight SQL server setup
    let addr: SocketAddr = "127.0.0.1:50051".parse()?;
    let flight_service = BasicFlightServiceImpl::new(); // Changed
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
