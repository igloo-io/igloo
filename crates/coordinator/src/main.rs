use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::util::pretty::print_batches;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use igloo_engine::QueryEngine;
use std::path::Path;
use std::sync::Arc; // For path operations

mod service;

use arrow_flight::flight_service_server::FlightServiceServer;
use igloo_api::IglooFlightSqlService;
use std::net::SocketAddr;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Instantiate the query engine
    let engine = QueryEngine::new();

    // 2. Register the Parquet file as a table
    // Path is relative to the workspace root where `cargo run` is executed.
    let parquet_relative_path = "crates/connectors/filesystem/test_data.parquet"; // Added crates/ prefix
    let base_path = std::env::current_dir()?; // This will be /app
    let tentative_path = base_path.join(Path::new(parquet_relative_path));
    let parquet_abs_path = tentative_path.canonicalize()?; // Canonicalize the path

    let table_path_url_str = format!("file://{}", parquet_abs_path.display()); // Ensure this is what DataFusion expects
    let table_path = ListingTableUrl::parse(&table_path_url_str)?;

    println!("Attempting to register test_table with path: {}", parquet_abs_path.display());

    let config = ListingTableConfig::new(table_path)
        .with_schema(Arc::new(Schema::new(vec![
            Field::new("col_a", DataType::Int64, false), // Matches col_a in parquet
            Field::new("col_b", DataType::Utf8, false),  // Matches col_b in parquet
        ])))
        .with_listing_options(
            ListingOptions::new(Arc::new(ParquetFormat::default())).with_file_extension(".parquet"),
        );

    let table_provider = Arc::new(ListingTable::try_new(config)?);
    engine.register_table("test_table", table_provider)?;
    println!("Registered test_table successfully.");

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
    let flight_service = IglooFlightSqlService::new(engine.clone()); // engine is cloned
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
