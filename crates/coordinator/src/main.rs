use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::util::pretty::print_batches;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use igloo_engine::QueryEngine;
use std::path::Path;
use std::sync::Arc;

// service and catalog modules are now part of the library, exposed by lib.rs
// main.rs will use them through the igloo_coordinator crate itself.
// No longer: mod service;

use arrow_flight::flight_service_server::FlightServiceServer;
use igloo_api::IglooFlightSqlService;
// If main.rs needed items from service module, it would be:
// use igloo_coordinator::service::MyCoordinatorService;
use igloo_common::catalog::MemoryCatalog;
use std::net::SocketAddr;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Instantiate the query engine and catalog
    let engine = Arc::new(QueryEngine::new());
    let mut catalog = MemoryCatalog::default();

    // 2. Register the CSV as a table in the catalog
    let csv_relative_path = "crates/connectors/filesystem/test_data.csv";
    let base_path = std::env::current_dir()?; // Assuming run from workspace root
    let csv_abs_path = base_path.join(Path::new(csv_relative_path));

    let table_path_url_str = format!("file://{}", csv_abs_path.display());
    let table_path = ListingTableUrl::parse(&table_path_url_str)?;

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
    catalog.register_table("test_table".to_string(), table_provider);
    println!("Registered test_table with the catalog.");

    // 3. Register tables from catalog with the engine
    for (name, table) in catalog.tables.iter() {
        engine.register_table(name, table.clone())?;
        println!("Registered table '{}' with the query engine.", name);
    }

    // 4. Execute a simple query
    let sql = "SELECT col_a, col_b FROM test_table LIMIT 5;";
    println!("Executing SQL: {}", sql);
    let results = engine.execute(sql).await;

    // 5. Print results
    println!("Query Results:");
    match print_batches(&results) {
        Ok(_) => {} // Already prints to stdout
        Err(e) => eprintln!("Error printing batches: {}", e),
    }
    println!("Finished printing query results.");

    // Keep the existing Flight SQL server setup
    let addr: SocketAddr = "127.0.0.1:50051".parse()?;
    let flight_service = IglooFlightSqlService::new(engine.clone(), Arc::new(catalog));
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
