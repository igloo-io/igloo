use anyhow::{anyhow, Context, Result};
use bincode;
use futures::future::try_join_all;

use igloo_api::pb::igloo_worker_client::IglooWorkerClient;
use igloo_api::pb::TaskDefinition;
use igloo_engine::logical_plan::LogicalPlan as EngineLogicalPlan; // Alias to avoid confusion
use igloo_engine::physical_plan::PhysicalPlan as EnginePhysicalPlan;
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// Plans a SQL query and returns a serialized physical plan.
///
/// This function performs basic SQL parsing and then creates a placeholder
/// physical plan. A real implementation would involve more sophisticated
/// logical and physical planning stages.
pub fn plan_query(sql: &str) -> Result<Vec<u8>> {
    let dialect = GenericDialect {};
    let ast_statements =
        Parser::parse_sql(&dialect, sql).map_err(|e| anyhow!("SQL parsing error: {:?}", e))?;

    let ast = ast_statements
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("No SQL statement provided to plan"))?;

    // Placeholder: Create a very simple logical plan (not used further for physical plan creation in this placeholder)
    let _logical_plan = EngineLogicalPlan::Dummy; // Or some basic interpretation of 'ast'

    // Placeholder: Create a physical plan based on basic SQL statement type
    let physical_plan = match ast {
        Statement::Query(_) => EnginePhysicalPlan::Scan {
            // Assuming SELECTs might become Scans
            table_name: "placeholder_table".to_string(), // Placeholder
            columns: vec!["*".to_string()],              // Placeholder
            predicate: None,                             // Placeholder
            partition_id: None,
            total_partitions: None,
        },
        _ => EnginePhysicalPlan::Dummy, // For other statement types like INSERT, UPDATE, etc.
    };

    bincode::serialize(&physical_plan).context("Failed to serialize physical plan")
}

/// Returns a hardcoded list of worker addresses.
///
/// In a real system, this would come from a discovery service, configuration,
/// or dynamic registration.
pub fn get_worker_addresses() -> Vec<String> {
    vec![
        "http://localhost:50051".to_string(),
        "http://localhost:50052".to_string(),
        "http://localhost:50053".to_string(),
    ]
}

/// Splits a physical plan into sub-plans for parallel execution.
///
/// Currently, only `Scan` plans are split. Other plan types are returned as is.
/// If `num_workers` is 0 or 1, the original plan is returned unchanged (cloned).
pub fn split_plan(plan: EnginePhysicalPlan, num_workers: usize) -> Vec<EnginePhysicalPlan> {
    if num_workers == 0 || num_workers == 1 {
        return vec![plan];
    }

    match plan {
        EnginePhysicalPlan::Scan {
            table_name,
            columns,
            predicate,
            .. // Ignore existing partition_id and total_partitions from the input plan
        } => {
            let mut split_plans = Vec::with_capacity(num_workers);
            for i in 0..num_workers {
                split_plans.push(EnginePhysicalPlan::Scan {
                    table_name: table_name.clone(),
                    columns: columns.clone(),
                    predicate: predicate.clone(),
                    partition_id: Some(i as u32),
                    total_partitions: Some(num_workers as u32),
                });
            }
            split_plans
        }
        _ => vec![plan.clone()], // Return other plan types as a single item vector
    }
}

/// Dispatches a query to multiple workers in parallel after splitting the plan.
pub async fn dispatch_parallel_task(sql: &str) -> Result<()> {
    // a. Get worker addresses
    let worker_addresses = get_worker_addresses();
    let num_workers = worker_addresses.len();

    if num_workers == 0 {
        return Err(anyhow!("No workers available to dispatch tasks."));
    }

    // b. Get original serialized plan and deserialize it
    let original_serialized_plan =
        plan_query(sql).context(format!("Failed to plan query for parallel dispatch: {}", sql))?;

    let original_physical_plan: EnginePhysicalPlan =
        bincode::deserialize(&original_serialized_plan)
            .context("Failed to deserialize original physical plan")?;

    // c. Split the physical plan
    let partitioned_plans = split_plan(original_physical_plan, num_workers);

    // Ensure we have a plan for each worker if the original was a Scan,
    // or one plan if it wasn't a Scan (it will be sent to the first worker).
    let mut plans_to_dispatch = Vec::new();
    if partitioned_plans.len() == 1
        && !matches!(partitioned_plans[0], EnginePhysicalPlan::Scan { .. })
    {
        // Non-scan plan, send to first worker
        for _ in 0..num_workers { // conceptually, only first worker gets it, but let's prepare for N futures
                                  // to simplify try_join_all, though only one would do real work
                                  // or, better, just send to one worker.
                                  // For this implementation, we'll send the single non-scan plan to each worker.
                                  // A more advanced scheduler might pick one.
            plans_to_dispatch.push(partitioned_plans[0].clone());
        }
    } else if partitioned_plans.len() == num_workers {
        plans_to_dispatch = partitioned_plans;
    } else if partitioned_plans.len() == 1
        && matches!(partitioned_plans[0], EnginePhysicalPlan::Scan { .. })
        && num_workers > 1
    {
        // This case means a Scan plan was not split when it should have been (num_workers > 1)
        // This shouldn't happen if split_plan works as expected.
        // However, if it does, we can choose to send this single scan plan to all workers,
        // or error. For simplicity, let's assume split_plan is correct and this path is less likely.
        // If split_plan returns 1 scan for N workers, it implies it decided not to split.
        // We will send this one plan to the first worker.
        plans_to_dispatch.push(partitioned_plans[0].clone());
        // And pad with Dummies for other workers if we want to send *something*
        // This part of logic needs refinement based on desired behavior for non-splittable scans.
        // For now, let's assume if split_plan returns 1 plan, it's intended for 1 worker or all.
        // The current split_plan logic for Scan with num_workers > 1 should always return N plans.
        // So, this path is more for non-Scan plans that result in 1 plan.
        // Re-evaluating: if it's a scan and not split, it's an issue.

        // Corrected logic: if it's a scan, it MUST be split into N, or it's an error if N > 1.
        // If it's not a scan, it's 1 plan, send to first worker.
        return Err(anyhow!(
            "Scan plan splitting did not result in the expected number of sub-plans. Expected {}, got {}.",
            num_workers,
            partitioned_plans.len()
        ));
    } else if partitioned_plans.is_empty() && num_workers > 0 {
        return Err(anyhow!("No plans generated to dispatch."));
    }

    // d. Create a vector of futures
    let mut futures_vec = Vec::new();

    // e. For each (partitioned_plan, worker_address) pair
    if plans_to_dispatch.len() == 1 && num_workers > 0 {
        // Single plan (e.g. non-scan, or scan for 1 worker)
        let plan_to_dispatch = plans_to_dispatch[0].clone();
        let worker_address = worker_addresses[0].clone(); // Send to the first worker

        let future = async move {
            let serialized_plan = bincode::serialize(&plan_to_dispatch)
                .context("Failed to serialize partitioned plan")?;
            let task_definition = TaskDefinition { plan: serialized_plan };

            let mut client = IglooWorkerClient::connect(worker_address.to_string())
                .await
                .context(format!("Failed to connect to worker: {}", worker_address))?;
            client
                .execute_task(tonic::Request::new(task_definition))
                .await
                .context(format!("Failed to dispatch task to worker: {}", worker_address))?;
            Ok(())
        };
        futures_vec.push(future);
    } else {
        // Multiple plans for multiple workers (Scan case)
        for (i, plan_to_dispatch) in plans_to_dispatch.into_iter().enumerate() {
            let worker_address = worker_addresses[i % num_workers].clone(); // Cycle if more plans than workers (should not happen for Scan)

            let future = async move {
                let serialized_plan = bincode::serialize(&plan_to_dispatch)
                    .context("Failed to serialize partitioned plan")?;
                let task_definition = TaskDefinition { plan: serialized_plan };

                let mut client = IglooWorkerClient::connect(worker_address.to_string())
                    .await
                    .context(format!("Failed to connect to worker: {}", worker_address))?;
                client
                    .execute_task(tonic::Request::new(task_definition))
                    .await
                    .context(format!("Failed to dispatch task to worker: {}", worker_address))?;
                Ok(())
            };
            futures_vec.push(future);
        }
    }

    if futures_vec.is_empty() && num_workers > 0 && !sql.trim().is_empty() {
        // This case might occur if the plan was Dummy and not added to plans_to_dispatch
        // and num_workers > 0. It's better to ensure Dummy plans are also "dispatched"
        // or handled explicitly.
        // For now, if we have workers but no futures, it's an issue unless SQL was empty.
        return Err(anyhow!("No tasks were prepared for dispatch."));
    } else if futures_vec.is_empty() && (num_workers == 0 || sql.trim().is_empty()) {
        // No workers or no SQL, so no tasks is fine.
        println!("No tasks to dispatch (no workers or empty SQL).");
        return Ok(());
    }

    // f. Use futures::future::try_join_all to execute concurrently
    match try_join_all(futures_vec).await {
        Ok(_) => {
            println!("All tasks dispatched successfully to workers.");
            Ok(())
        }
        Err(e) => {
            eprintln!("An error occurred while dispatching tasks: {:?}", e);
            Err(e)
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use igloo_engine::logical_plan::Expression as EngineExpression; // For creating predicate in test

    #[test]
    fn test_plan_query_simple_select() {
        let sql = "SELECT a FROM my_table";
        let result = plan_query(sql);
        assert!(result.is_ok(), "plan_query failed for simple select: {:?}", result.err());
        let serialized_plan = result.unwrap();
        assert!(!serialized_plan.is_empty());

        // Optional: deserialize and check plan type
        let plan: Result<EnginePhysicalPlan, _> = bincode::deserialize(&serialized_plan);
        assert!(plan.is_ok(), "Failed to deserialize plan from plan_query");
        match plan.unwrap() {
            EnginePhysicalPlan::Scan { table_name, .. } => {
                assert_eq!(table_name, "placeholder_table")
            }
            _ => panic!("Expected a Scan plan for SELECT query"),
        }
    }

    #[test]
    fn test_split_scan_plan() {
        let scan_plan = EnginePhysicalPlan::Scan {
            table_name: "test_table".to_string(),
            columns: vec!["col1".to_string(), "col2".to_string()],
            predicate: Some(EngineExpression::Dummy),
            partition_id: None,
            total_partitions: None,
        };

        let num_workers = 3;
        let split_plans = split_plan(scan_plan, num_workers);

        assert_eq!(split_plans.len(), num_workers);
        for (i, p) in split_plans.iter().enumerate() {
            if let EnginePhysicalPlan::Scan { partition_id, total_partitions, .. } = p {
                assert_eq!(*partition_id, Some(i as u32));
                assert_eq!(*total_partitions, Some(num_workers as u32));
            } else {
                panic!("Expected a Scan plan after splitting");
            }
        }
    }

    #[test]
    fn test_split_non_scan_plan() {
        let dummy_plan = EnginePhysicalPlan::Dummy;
        let num_workers = 3;
        let split_plans = split_plan(dummy_plan.clone(), num_workers);

        assert_eq!(split_plans.len(), 1);
        assert_eq!(split_plans[0], dummy_plan);
    }

    #[test]
    fn test_split_plan_with_one_worker() {
        let scan_plan = EnginePhysicalPlan::Scan {
            table_name: "test_table".to_string(),
            columns: vec!["col1".to_string()],
            predicate: None,
            partition_id: None,
            total_partitions: None,
        };
        let num_workers = 1;
        let split_plans = split_plan(scan_plan.clone(), num_workers);
        assert_eq!(split_plans.len(), 1);
        if let EnginePhysicalPlan::Scan { partition_id, total_partitions, .. } = &split_plans[0] {
            assert!(partition_id.is_none()); // Should be original plan
            assert!(total_partitions.is_none());
        } else {
            panic!("Expected a Scan plan");
        }
    }

    #[test]
    fn test_split_plan_with_zero_workers() {
        let scan_plan = EnginePhysicalPlan::Scan {
            table_name: "test_table".to_string(),
            columns: vec!["col1".to_string()],
            predicate: None,
            partition_id: None,
            total_partitions: None,
        };
        let num_workers = 0;
        let split_plans = split_plan(scan_plan.clone(), num_workers);
        assert_eq!(split_plans.len(), 1);
        if let EnginePhysicalPlan::Scan { partition_id, total_partitions, .. } = &split_plans[0] {
            assert!(partition_id.is_none()); // Should be original plan
            assert!(total_partitions.is_none());
        } else {
            panic!("Expected a Scan plan");
        }
    }
}
