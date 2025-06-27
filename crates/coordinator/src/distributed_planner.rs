//! Distributed physical planner for breaking plans into fragments

use crate::fragment::{QueryFragment, FragmentType};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::LogicalPlan;
use igloo_engine::PhysicalPlanner;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

/// Distributed planner that creates query fragments for worker execution
pub struct DistributedPlanner {
    physical_planner: PhysicalPlanner,
    worker_addresses: Vec<String>,
}

impl DistributedPlanner {
    pub fn new(physical_planner: PhysicalPlanner, worker_addresses: Vec<String>) -> Self {
        Self {
            physical_planner,
            worker_addresses,
        }
    }

    pub async fn create_distributed_plan(
        &self,
        logical_plan: &LogicalPlan,
    ) -> DataFusionResult<Vec<QueryFragment>> {
        let mut fragments = Vec::new();
        let mut fragment_map = HashMap::new();
        
        self.create_fragments_recursive(logical_plan, &mut fragments, &mut fragment_map).await?;
        
        Ok(fragments)
    }

    async fn create_fragments_recursive(
        &self,
        plan: &LogicalPlan,
        fragments: &mut Vec<QueryFragment>,
        fragment_map: &mut HashMap<String, String>, // plan_id -> fragment_id
    ) -> DataFusionResult<String> {
        match plan {
            LogicalPlan::TableScan(scan) => {
                // Create a scan fragment for each table
                let fragment_id = Uuid::new_v4().to_string();
                let worker_address = self.select_worker_for_table(&scan.table_name);
                
                let physical_plan = self.physical_planner.create_physical_plan(plan).await?;
                
                let fragment = QueryFragment {
                    id: fragment_id.clone(),
                    fragment_type: FragmentType::Scan,
                    physical_plan,
                    worker_address,
                    dependencies: Vec::new(),
                };
                
                fragments.push(fragment);
                fragment_map.insert(format!("{:?}", plan), fragment_id.clone());
                
                Ok(fragment_id)
            }
            
            LogicalPlan::Join(join) => {
                // Create fragments for left and right inputs
                let left_fragment_id = self.create_fragments_recursive(
                    &join.left, fragments, fragment_map
                ).await?;
                let right_fragment_id = self.create_fragments_recursive(
                    &join.right, fragments, fragment_map
                ).await?;
                
                // Create a join fragment that depends on both inputs
                let fragment_id = Uuid::new_v4().to_string();
                let coordinator_address = "coordinator".to_string(); // Join happens on coordinator
                
                let physical_plan = self.physical_planner.create_physical_plan(plan).await?;
                
                let fragment = QueryFragment {
                    id: fragment_id.clone(),
                    fragment_type: FragmentType::Join,
                    physical_plan,
                    worker_address: coordinator_address,
                    dependencies: vec![left_fragment_id, right_fragment_id],
                };
                
                fragments.push(fragment);
                fragment_map.insert(format!("{:?}", plan), fragment_id.clone());
                
                Ok(fragment_id)
            }
            
            LogicalPlan::Projection(projection) => {
                // Projections can often be pushed down to the input fragment
                let input_fragment_id = self.create_fragments_recursive(
                    &projection.input, fragments, fragment_map
                ).await?;
                
                // For simplicity, create a separate projection fragment
                let fragment_id = Uuid::new_v4().to_string();
                let worker_address = self.worker_addresses[0].clone(); // Use first worker
                
                let physical_plan = self.physical_planner.create_physical_plan(plan).await?;
                
                let fragment = QueryFragment {
                    id: fragment_id.clone(),
                    fragment_type: FragmentType::Compute,
                    physical_plan,
                    worker_address,
                    dependencies: vec![input_fragment_id],
                };
                
                fragments.push(fragment);
                fragment_map.insert(format!("{:?}", plan), fragment_id.clone());
                
                Ok(fragment_id)
            }
            
            LogicalPlan::Filter(filter) => {
                // Filters can often be pushed down to the input fragment
                let input_fragment_id = self.create_fragments_recursive(
                    &filter.input, fragments, fragment_map
                ).await?;
                
                // For simplicity, create a separate filter fragment
                let fragment_id = Uuid::new_v4().to_string();
                let worker_address = self.worker_addresses[0].clone(); // Use first worker
                
                let physical_plan = self.physical_planner.create_physical_plan(plan).await?;
                
                let fragment = QueryFragment {
                    id: fragment_id.clone(),
                    fragment_type: FragmentType::Compute,
                    physical_plan,
                    worker_address,
                    dependencies: vec![input_fragment_id],
                };
                
                fragments.push(fragment);
                fragment_map.insert(format!("{:?}", plan), fragment_id.clone());
                
                Ok(fragment_id)
            }
            
            _ => Err(DataFusionError::NotImplemented(
                format!("Distributed planning for {:?} not implemented", plan)
            )),
        }
    }

    fn select_worker_for_table(&self, table_name: &str) -> String {
        // Simple round-robin selection based on table name hash
        let hash = table_name.chars().map(|c| c as usize).sum::<usize>();
        let worker_index = hash % self.worker_addresses.len();
        self.worker_addresses[worker_index].clone()
    }
}