use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use igloo_engine::QueryEngine;
use std::path::Path;
use std::sync::Arc;
use std::net::SocketAddr;

use arrow_flight::flight_service_server::FlightServiceServer;
use igloo_api::IglooFlightSqlService;
use crate::service::CoordinatorFlightSqlService; // Adjusted path
use tonic::transport::Server;

pub mod service;

// Extracted server launch logic
pub async fn launch_coordinator(addr_override: Option<SocketAddr>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // 1. Instantiate the query engine
    let engine = QueryEngine::new();

    // 2. Register the CSV as a table (optional, could be moved or made configurable)
    let csv_relative_path = "../connectors/filesystem/test_data.csv"; // Relative to workspace root
    let base_path = std::env::current_dir()?;
    let csv_abs_path = base_path.join(Path::new(csv_relative_path));
    let table_path_url_str = format!("file://{}", csv_abs_path.display());
    let table_path = ListingTableUrl::parse(&table_path_url_str)?;
    println!("Attempting to register test_table from lib.rs with path: {}", csv_abs_path.display());
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
    println!("Registered test_table successfully from lib.rs.");

    // Determine address: override if Some, else default.
    let addr: SocketAddr = addr_override.unwrap_or_else(|| "127.0.0.1:50051".parse().unwrap());

    let igloo_flight_service = IglooFlightSqlService::new(engine.clone());
    let coordinator_service = CoordinatorFlightSqlService::new(igloo_flight_service);

    println!("Flight SQL server (from lib.rs) listening on {}", addr);
    Server::builder()
        .add_service(FlightServiceServer::new(coordinator_service))
        .serve_with_shutdown(addr, async {
            tokio::signal::ctrl_c().await.expect("Failed to listen for CTRL+C signal");
            println!("Shutting down Flight SQL server (from lib.rs)...");
        })
        .await?;

    println!("Flight SQL server (from lib.rs) shut down gracefully.");
    Ok(())
}
