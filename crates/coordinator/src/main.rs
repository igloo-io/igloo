use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::util::pretty::print_batches;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use igloo_engine::QueryEngine;
use std::sync::Arc;

mod service;

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

    // 3. Run the query
    let sql = "SELECT col_a, col_b FROM test_table LIMIT 5;";
    let batches = engine.execute(sql).await;

    // 4. Print the results
    print_batches(&batches)?;
    Ok(())

    // let addr: SocketAddr = "127.0.0.1:50051".parse()?;
    // let cluster: ClusterState = Arc::new(Mutex::new(HashMap::new()));
    // let svc = MyCoordinatorService { cluster };
    // let igloo_flight_sql_service_instance = IglooFlightSqlService {}; // Assuming a simple struct instantiation for now
    //                                                                   // Start gRPC server with graceful shutdown
    // println!("Coordinator listening on {}", addr);
    // Server::builder()
    //     .add_service(CoordinatorServiceServer::new(svc)) // Existing service
    //     .add_service(FlightServiceServer::new(igloo_flight_sql_service_instance)) // New Flight SQL service
    //     .serve_with_shutdown(addr, async {
    //         signal::ctrl_c().await.expect("failed to listen for event");
    //         println!("Shutting down coordinator gracefully...");
    //     })
    //     .await?;
    // Ok(())
}
