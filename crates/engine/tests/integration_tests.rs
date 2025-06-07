// crates/engine/tests/integration_tests.rs
use igloo_engine::physical_plan::PhysicalPlan;
use igloo_engine::planner::{Expression, LogicalPlan, Operator, PhysicalPlanner};
use igloo_engine::ExecutionError;

use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use futures::TryStreamExt; // For collecting stream results
use std::sync::Arc;

// Helper to get the dummy schema, consistent with ScanExec's output & PhysicalPlanner
fn get_dummy_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]))
}

#[tokio::test]
async fn test_scan_execution() -> Result<(), ExecutionError> {
    let schema = get_dummy_schema();
    let scan_plan = PhysicalPlan::ScanExec {
        schema: Arc::clone(&schema),
    };

    let stream = scan_plan.execute();
    let results: Vec<RecordBatch> = stream.try_collect().await?;

    assert_eq!(results.len(), 2, "ScanExec should produce 2 batches");

    // Check first batch (as per ScanExec current dummy data)
    // id: [1, 2, 3, 6, 7], name: ["Alice", "Bob", "Charlie", "FilterMe", "KeepMe"]
    assert_eq!(results[0].num_rows(), 5);
    let id_col_batch0 = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(id_col_batch0.values(), &[1, 2, 3, 6, 7]);

    // Check second batch
    // id: [4, 5, 8], name: ["David", "Eve", "Another"]
    assert_eq!(results[1].num_rows(), 3);
    let id_col_batch1 = results[1]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(id_col_batch1.values(), &[4, 5, 8]);

    let total_rows: usize = results.iter().map(|rb| rb.num_rows()).sum();
    assert_eq!(total_rows, 8);


    Ok(())
}

#[tokio::test]
async fn test_filter_execution() -> Result<(), ExecutionError> {
    let schema = get_dummy_schema();
        let scan_plan = Arc::new(PhysicalPlan::ScanExec { // Added PhysicalPlan here for clarity
        schema: Arc::clone(&schema),
    });

    // Predicate: id > 3 (uses our simple evaluator)
    let predicate = Expression::BinaryExpr {
        left: Box::new(Expression::Column("id".to_string())),
        op: Operator::Gt,
        right: Box::new(Expression::Literal("3".to_string())),
    };

    let filter_plan = PhysicalPlan::FilterExec {
        predicate,
        input: scan_plan,
        schema: Arc::clone(&schema), // Filter output schema is same as input
    };

    let stream = filter_plan.execute();
    let results: Vec<RecordBatch> = stream.try_collect().await?;

    // Expected rows after filtering (id > 3):
    // Batch 1 originally: [1, 2, 3, 6, 7] -> filter -> [6, 7]
    // Batch 2 originally: [4, 5, 8] -> filter -> [4, 5, 8]
    // Total = 2 + 3 = 5 rows

    let total_rows: usize = results.iter().map(|rb| rb.num_rows()).sum();
    assert_eq!(total_rows, 5, "Filter should result in 5 rows for id > 3");

    // Further checks can be done on the content if necessary
    // For example, collect all 'id' values and check them.
    let mut all_ids: Vec<i32> = Vec::new(); // Changed to Vec<i32>
    for batch in results {
        let id_col = batch.column_by_name("id").unwrap().as_any().downcast_ref::<Int32Array>().unwrap();
        all_ids.extend(id_col.values().iter().copied()); // Added .copied()
    }
    all_ids.sort(); // Ensure order for comparison
    assert_eq!(all_ids, vec![4, 5, 6, 7, 8]); // Compare with i32 values


    Ok(())
}

#[tokio::test]
async fn test_physical_planner_with_execution() -> Result<(), anyhow::Error> {
    // 1. Create Logical Plan (Filter (id > 3) -> Scan)
        let logical_scan = LogicalPlan::TableScan { table_name: "dummy_table".to_string() }; // Added LogicalPlan for clarity
    let logical_filter = LogicalPlan::Filter {
        input: Box::new(logical_scan),
        predicate: Expression::BinaryExpr {
            left: Box::new(Expression::Column("id".to_string())),
            op: Operator::Gt,
            right: Box::new(Expression::Literal("3".to_string())),
        }
    };

    // 2. Create Physical Plan using PhysicalPlanner
    let planner = PhysicalPlanner::new();
    let physical_plan = planner.create_physical_plan(&logical_filter)?; // Handles PlanningError

    // 3. Execute Physical Plan
    let stream = physical_plan.execute();
    let results: Vec<RecordBatch> = stream.try_collect().await?; // Handles ExecutionError

    // 4. Assert Results (same assertions as test_filter_execution)
    let total_rows: usize = results.iter().map(|rb| rb.num_rows()).sum();
    assert_eq!(total_rows, 5, "Planner+Filter should result in 5 rows for id > 3");

    let mut all_ids: Vec<i32> = Vec::new(); // Changed to Vec<i32>
    for batch in results {
        let id_col = batch.column_by_name("id").unwrap().as_any().downcast_ref::<Int32Array>().unwrap();
        all_ids.extend(id_col.values().iter().copied()); // Added .copied()
    }
    all_ids.sort();
    assert_eq!(all_ids, vec![4, 5, 6, 7, 8]); // Compare with i32 values

    Ok(())
}
