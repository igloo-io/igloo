// crates/coordinator/src/scheduler.rs
#[allow(unused_imports)] // Expression is used via PhysicalPlan fields and in tests
use igloo_engine::{Expression, PhysicalPlan}; // Corrected import path
use std::sync::Arc;

/// Splits a PhysicalPlan for parallel execution based on Scan nodes.
///
/// - `plan`: The physical plan to split.
/// - `num_partitions`: The desired number of partitions (e.g., number of workers).
///
/// Returns a Vec of PhysicalPlans, one for each partition.
/// If the plan cannot be split (e.g., no Scan node or num_partitions <= 1),
/// it returns a Vec containing the original plan.
#[allow(dead_code)] // Will be used later by the coordinator service
pub fn split_plan(plan: Arc<PhysicalPlan>, num_partitions: usize) -> Vec<Arc<PhysicalPlan>> {
    if num_partitions <= 1 {
        return vec![plan];
    }

    // This is a recursive function to find and split Scan nodes.
    fn split_node_recursive(
        node: Arc<PhysicalPlan>,
        num_partitions: usize,
        is_top_level_split: bool, // Flag to ensure we only split one level of Scans
    ) -> Vec<Arc<PhysicalPlan>> {
        match node.as_ref() {
            PhysicalPlan::Scan {
                table_name,
                columns,
                predicate,
                // Ignore existing partition_id, total_partitions for splitting decision
                ..
            } if is_top_level_split => {
                let mut partitioned_scans = Vec::new();
                for i in 0..num_partitions {
                    let new_scan = PhysicalPlan::Scan {
                        table_name: table_name.clone(),
                        columns: columns.clone(),
                        predicate: predicate.clone(), // Expression needs to be Clone
                        partition_id: Some(i),
                        total_partitions: Some(num_partitions),
                    };
                    partitioned_scans.push(Arc::new(new_scan));
                }
                partitioned_scans
            }
            PhysicalPlan::Filter { input, predicate } => {
                // Recursively try to split the input of the filter
                let split_inputs =
                    split_node_recursive(Arc::clone(input), num_partitions, is_top_level_split);
                if split_inputs.len() > 1 && Arc::ptr_eq(input, &split_inputs[0]) {
                    // Input wasn't a splittable scan, or already split deeper. Propagate single node.
                    vec![node]
                } else {
                    // Input was split, so create a Filter for each split part
                    split_inputs
                        .into_iter()
                        .map(|new_input| {
                            Arc::new(PhysicalPlan::Filter {
                                input: new_input,
                                predicate: predicate.clone(), // Expression needs to be Clone
                            })
                        })
                        .collect()
                }
            }
            PhysicalPlan::Projection { input, expressions } => {
                // Recursively try to split the input of the projection
                let split_inputs =
                    split_node_recursive(Arc::clone(input), num_partitions, is_top_level_split);
                if split_inputs.len() > 1 && Arc::ptr_eq(input, &split_inputs[0]) {
                    vec![node]
                } else {
                    split_inputs
                        .into_iter()
                        .map(|new_input| {
                            Arc::new(PhysicalPlan::Projection {
                                input: new_input,
                                expressions: expressions.clone(), // Vec<(Expression, String)> needs to be Clone
                            })
                        })
                        .collect()
                }
            }
            // For other node types (Dummy, etc.), or if not splitting this scan (is_top_level_split = false)
            // return the node as is, wrapped in a Vec.
            _ => vec![node],
        }
    }

    // Attempt to split the plan starting from the root.
    // The `is_top_level_split = true` ensures we only split the *first encountered* set of Scans.
    // If a plan has multiple scans at different levels, this simple logic will only parallelize one.
    // More advanced schedulers might handle this differently.
    split_node_recursive(plan, num_partitions, true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use igloo_engine::{Expression, PhysicalPlan}; // Corrected import path
    use std::sync::Arc;

    // Helper to create a simple Scan plan
    fn test_scan_plan(table: &str) -> Arc<PhysicalPlan> {
        Arc::new(PhysicalPlan::Scan {
            table_name: table.to_string(),
            columns: vec!["col_a".to_string()],
            predicate: None,
            partition_id: None,
            total_partitions: None,
        })
    }

    // Helper to create a placeholder expression
    fn placeholder_expr(val: &str) -> Expression {
        Expression::Placeholder(val.to_string())
    }

    #[test]
    fn test_split_simple_scan() {
        let plan = test_scan_plan("test_table");
        let num_partitions = 3;
        let split_plans = split_plan(plan, num_partitions);

        assert_eq!(split_plans.len(), num_partitions);
        for (i, current_plan) in split_plans.iter().enumerate().take(num_partitions) {
            if let PhysicalPlan::Scan { table_name, partition_id, total_partitions, .. } =
                current_plan.as_ref()
            {
                assert_eq!(table_name, "test_table");
                assert_eq!(*partition_id, Some(i)); // Dereferenced
                assert_eq!(*total_partitions, Some(num_partitions)); // Dereferenced
            } else {
                panic!("Expected a Scan plan, got {:?}", current_plan);
            }
        }
    }

    #[test]
    fn test_split_scan_with_filter_projection() {
        let plan = Arc::new(PhysicalPlan::Projection {
            input: Arc::new(PhysicalPlan::Filter {
                input: test_scan_plan("test_table"),
                predicate: placeholder_expr("col_a > 10"),
            }),
            expressions: vec![(placeholder_expr("col_a"), "alias_a".to_string())],
        });
        let num_partitions = 2;
        let split_plans = split_plan(plan, num_partitions);

        assert_eq!(split_plans.len(), num_partitions);
        for (i, current_plan) in split_plans.iter().enumerate().take(num_partitions) {
            // Check structure: Projection -> Filter -> Scan
            if let PhysicalPlan::Projection { input: proj_input, expressions } =
                current_plan.as_ref()
            {
                assert_eq!(expressions.len(), 1);
                if let PhysicalPlan::Filter { input: filter_input, predicate } = proj_input.as_ref()
                {
                    assert_eq!(*predicate, placeholder_expr("col_a > 10"));
                    if let PhysicalPlan::Scan {
                        table_name, partition_id, total_partitions, ..
                    } = filter_input.as_ref()
                    {
                        assert_eq!(table_name, "test_table");
                        assert_eq!(*partition_id, Some(i)); // Dereferenced
                        assert_eq!(*total_partitions, Some(num_partitions)); // Dereferenced
                    } else {
                        panic!("Expected Scan at the core, got {:?}", filter_input);
                    }
                } else {
                    panic!("Expected Filter, got {:?}", proj_input);
                }
            } else {
                panic!("Expected Projection, got {:?}", current_plan);
            }
        }
    }

    #[test]
    fn test_split_no_scan() {
        let plan = Arc::new(PhysicalPlan::Dummy);
        let num_partitions = 3;
        let split_plans = split_plan(plan.clone(), num_partitions); // Cloned plan
        assert_eq!(split_plans.len(), 1);
        assert!(Arc::ptr_eq(&plan, &split_plans[0])); // Should be the same plan
    }

    #[test]
    fn test_split_num_partitions_one() {
        let plan = test_scan_plan("test_table");
        let num_partitions = 1;
        let split_plans = split_plan(plan.clone(), num_partitions); // Cloned plan
        assert_eq!(split_plans.len(), 1);
        // In this case, it should return the original plan without partition info
        if let PhysicalPlan::Scan { partition_id, total_partitions, .. } = split_plans[0].as_ref() {
            assert!(partition_id.is_none());
            assert!(total_partitions.is_none());
        } else {
            panic!("Expected a Scan plan, got {:?}", split_plans[0]);
        }
    }
}
