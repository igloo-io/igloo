use clap::Parser;
use datafusion::arrow::util::pretty::print_batches;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::LogicalPlan;
use igloo_common::catalog::MemoryCatalog;
use igloo_engine::{QueryEngine, PhysicalPlanner};
use std::sync::Arc;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    config: Option<String>,
    
    #[arg(short, long)]
    sql: Option<String>,
    
    #[arg(long)]
    distributed: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    println!("Igloo Query Engine CLI (Main Crate)");
    
    if let Some(sql) = args.sql {
        if args.distributed {
            println!("Executing distributed query: {}", sql);
            execute_distributed_query(&sql).await?;
        } else {
            println!("Executing local query: {}", sql);
            execute_local_query(&sql).await?;
        }
    } else if let Some(config_path) = args.config {
        println!("Config file specified: {}", config_path);
        // Here you would load the configuration and start the appropriate service
        // e.g., coordinator, worker, or client based on the config or other args
    } else {
        println!("No config file specified. Starting in default mode or showing help.");
        // Demonstrate with a sample query
        let sample_sql = "SELECT 42 as answer, 'Hello Igloo' as message";
        println!("Running sample query: {}", sample_sql);
        execute_local_query(sample_sql).await?;
    }

    // Example: use a function from the lib part of this crate
    println!("{}", igloo::hello());

    Ok(())
}

async fn execute_local_query(sql: &str) -> Result<(), Box<dyn std::error::Error>> {
    // Initialize catalog and register sample data
    let mut catalog = MemoryCatalog::new();
    
    // Create a simple in-memory table for demonstration
    use datafusion::arrow::array::{Int32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::catalog::MemTable;
    
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    
    let ids = Int32Array::from(vec![1, 2, 3, 4, 5]);
    let names = StringArray::from(vec!["Alice", "Bob", "Charlie", "Diana", "Eve"]);
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(ids), Arc::new(names)],
    )?;
    
    let table_provider = Arc::new(MemTable::try_new(schema, vec![vec![batch]])?);
    catalog.register_table("users".to_string(), table_provider);
    
    // Initialize query engine
    let engine = QueryEngine::new();
    
    // Register tables with engine
    for (name, table) in catalog.tables.iter() {
        engine.register_table(name, table.clone())?;
    }
    
    // Execute query using DataFusion directly for now
    let results = engine.execute(sql).await;
    
    // Print results
    println!("Query Results:");
    print_batches(&results)?;
    
    Ok(())
}

async fn execute_distributed_query(sql: &str) -> Result<(), Box<dyn std::error::Error>> {
    println!("Distributed execution not yet implemented. Falling back to local execution.");
    execute_local_query(sql).await
}

async fn _execute_with_physical_planner(sql: &str) -> Result<(), Box<dyn std::error::Error>> {
    // Initialize components
    let catalog = Arc::new(MemoryCatalog::new());
    let session_ctx = Arc::new(SessionContext::new());
    let planner = PhysicalPlanner::new(catalog.clone(), session_ctx.clone());
    
    // Parse SQL to logical plan
    let logical_plan = session_ctx.sql(sql).await?.into_optimized_plan()?;
    
    // Convert to physical plan
    let physical_plan = planner.create_physical_plan(&logical_plan).await?;
    
    // Execute physical plan
    let mut stream = physical_plan.execute().await?;
    let mut batches = Vec::new();
    
    use futures::StreamExt;
    while let Some(batch_result) = stream.next().await {
        batches.push(batch_result?);
    }
    
    // Print results
    println!("Query Results:");
    print_batches(&batches)?;
    
    Ok(())
}