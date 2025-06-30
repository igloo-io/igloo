//! Query fragment representation for distributed execution

use igloo_engine::physical_plan::ExecutionPlan;
use std::sync::Arc;

/// Type of query fragment
#[derive(Debug, Clone, PartialEq)]
pub enum FragmentType {
    Scan,     // Table scan operation
    Join,     // Join operation
    Compute,  // General computation (projection, filter, etc.)
    Shuffle,  // Data redistribution
}

/// A query fragment that can be executed on a worker
#[derive(Debug)]
pub struct QueryFragment {
    pub id: String,
    pub fragment_type: FragmentType,
    pub physical_plan: Arc<dyn ExecutionPlan>,
    pub worker_address: String,
    pub dependencies: Vec<String>, // Fragment IDs this fragment depends on
}

impl Clone for QueryFragment {
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            fragment_type: self.fragment_type.clone(),
            physical_plan: self.physical_plan.clone(),
            worker_address: self.worker_address.clone(),
            dependencies: self.dependencies.clone(),
        }
    }
}

impl QueryFragment {
    pub fn new(
        id: String,
        fragment_type: FragmentType,
        physical_plan: Arc<dyn ExecutionPlan>,
        worker_address: String,
        dependencies: Vec<String>,
    ) -> Self {
        Self {
            id,
            fragment_type,
            physical_plan,
            worker_address,
            dependencies,
        }
    }

    pub fn is_ready(&self, completed_fragments: &std::collections::HashSet<String>) -> bool {
        self.dependencies.iter().all(|dep| completed_fragments.contains(dep))
    }
}