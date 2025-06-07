// This file defines the StagePlanner for breaking down a DataFusion ExecutionPlan into QueryStages.

use crate::stage::QueryStage;
use datafusion::physical_plan::{
    repartition::RepartitionExec, ExecutionPlan, ExecutionPlanProperties,
};
use std::sync::Arc;

pub struct StagePlanner;

impl StagePlanner {
    pub fn plan_stages(
        &self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Vec<QueryStage>, String> {
        let mut stages = Vec::new();
        let mut stage_id_counter: usize = 0;
        let mut root_input_stage_ids = Vec::new();

        // Walk the plan recursively to identify stages
        self.walk_plan_recursive(
            plan.clone(),
            &mut stages,
            &mut stage_id_counter,
            &mut root_input_stage_ids,
        );

        // Create the final stage (root stage)
        stage_id_counter += 1;
        let final_stage_id = format!("stage_{}", stage_id_counter);
        let final_stage = QueryStage {
            stage_id: final_stage_id,
            plan_fragment: Vec::new(), // Placeholder
            input_stage_ids: root_input_stage_ids, // Inputs collected from children
            output_partitions: plan.output_partitioning().partition_count(),
        };
        stages.push(final_stage);

        // The stages are identified in a child-first (post-order) traversal.
        // For execution, it's often more natural to think about them in dependency order.
        // Reversing the list can achieve this, though true topological sort might be needed for complex scenarios.
        stages.reverse();

        Ok(stages)
    }

    fn walk_plan_recursive(
        &self,
        current_plan_node: Arc<dyn ExecutionPlan>,
        stages: &mut Vec<QueryStage>,
        stage_id_counter: &mut usize,
        // Collects output stage IDs from children that become inputs to the current_plan_node's *parent* stage
        // or inputs to the new stage if current_plan_node is a RepartitionExec.
        parent_stage_inputs: &mut Vec<String>,
    ) {
        // Inputs for the *current* node's potential new stage.
        // These are collected from children that themselves become stage boundaries.
        let mut current_node_stage_inputs = Vec::new();

        for child_plan in current_plan_node.children() {
            // Recursively walk children.
            // Inputs propagated from a child RepartitionExec will be added to current_node_stage_inputs.
            self.walk_plan_recursive(
                child_plan.clone(),
                stages,
                stage_id_counter,
                &mut current_node_stage_inputs, // Children will add their stage IDs here if they form new stages
            );
        }

        // Check if the current node is a stage boundary (RepartitionExec)
        if let Some(repartition_exec) = current_plan_node
            .as_any()
            .downcast_ref::<RepartitionExec>()
        {
            // This node marks the beginning of a new stage.
            *stage_id_counter += 1;
            let new_stage_id = format!("stage_{}", *stage_id_counter);

            let new_stage = QueryStage {
                stage_id: new_stage_id.clone(),
                plan_fragment: Vec::new(), // Placeholder for the serialized plan of this stage
                // The inputs to this new stage are the stages created by its children
                // that were themselves RepartitionExec (collected in current_node_stage_inputs).
                input_stage_ids: current_node_stage_inputs.clone(),
                output_partitions: repartition_exec.partitioning().partition_count(),
            };
            stages.push(new_stage);

            // This new stage's ID becomes an input to its parent's stage.
            // Clear current_node_stage_inputs as they've been consumed by this new stage,
            // and add this new stage's ID to be propagated upwards.
            parent_stage_inputs.clear(); // Clear inputs from deeper children, this stage consumes them.
            parent_stage_inputs.push(new_stage_id);
        } else {
            // This node is part of the same stage as its parent.
            // Propagate any stage IDs collected from its children (if they were RepartitionExec)
            // upwards to the parent_stage_inputs.
            // Example: Proj -> Repart -> Scan.
            // Scan is processed, no inputs.
            // Repart is processed, creates stage_1, parent_stage_inputs for Proj becomes ["stage_1"].
            // Proj is processed, it's not a Repart. It should pass ["stage_1"] upwards.
            // So, we extend parent_stage_inputs with what current_node_stage_inputs has collected.
            parent_stage_inputs.extend(current_node_stage_inputs);
        }
    }
}
