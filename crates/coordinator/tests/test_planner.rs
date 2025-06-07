#[cfg(test)]
mod tests {
    use igloo_coordinator::planner::StagePlanner;
    use igloo_coordinator::stage::QueryStage; // Now needed for assertions
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::repartition::RepartitionExec;
    use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties, Partitioning}; // ExecutionPlanProperties might be useful
    use datafusion::arrow::datatypes::{Schema, SchemaRef};
    use std::sync::Arc;
    // use datafusion::physical_expr::PhysicalSortExpr; // Not used yet
    // use datafusion::physical_plan::projection::ProjectionExec; // Not used yet

    // Helper to create a basic schema
    fn an_empty_schema() -> SchemaRef {
        Arc::new(Schema::empty())
    }

    // Helper to create a basic plan (e.g., EmptyExec)
    fn an_empty_plan() -> Arc<dyn ExecutionPlan> {
        Arc::new(EmptyExec::new(false, an_empty_schema()))
    }

    #[test]
    fn test_plan_with_no_shuffles() {
        let planner = StagePlanner;
        let plan = an_empty_plan(); // Simplest plan

        let stages_result = planner.plan_stages(plan.clone()); // Pass clone if plan is used later
        assert!(stages_result.is_ok());
        let stages = stages_result.unwrap();

        // Expected: One stage (the root stage)
        assert_eq!(stages.len(), 1, "Should create one stage for a plan with no shuffles. Stages: {:?}", stages);
        let stage0 = &stages[0];
        assert_eq!(stage0.input_stage_ids.len(), 0, "Root stage should have no input stages. Stage: {:?}", stage0);
        assert_eq!(stage0.output_partitions, plan.output_partitioning().partition_count(), "Root stage output partitions should match plan. Stage: {:?}", stage0);
        // Check stage ID format (basic check)
        assert!(stage0.stage_id.starts_with("stage_"), "Stage ID should have 'stage_' prefix: {}", stage0.stage_id);
    }

    #[test]
    fn test_plan_with_one_shuffle() {
        let planner = StagePlanner;
        let input_plan = an_empty_plan(); // Leaf node
        let num_output_partitions_repart = 8;

        let plan = Arc::new(
            RepartitionExec::try_new(
                input_plan.clone(),
                Partitioning::RoundRobinBatch(num_output_partitions_repart),
            )
            .unwrap(),
        );

        let stages_result = planner.plan_stages(plan.clone());
        assert!(
            stages_result.is_ok(),
            "Planning failed: {:?}",
            stages_result.err()
        );
        let stages = stages_result.unwrap();

        // Expected: Two stages.
        // Stage 0: from EmptyExec (leaf, becomes a stage because its parent is RepartitionExec)
        // Stage 1: from RepartitionExec (consumes Stage 0)
        assert_eq!(stages.len(), 2, "Should create two stages for a plan with one shuffle. Stages: {:?}", stages);

        // Order by planner: EmptyExec's stage (child) first, then RepartitionExec's stage (parent/root of this sub-plan)
        // The planner reverses them, so stage_0 is from EmptyExec, stage_1 is from RepartitionExec

        let stage0 = &stages[0]; // Stage from EmptyExec (implicit stage for the Repartition input)
        let stage1 = &stages[1]; // Stage from RepartitionExec

        // Stage 0 (from EmptyExec - becomes the first stage feeding the RepartitionExec)
        // This is the "final stage" created for the input of RepartitionExec
        assert_eq!(stage0.input_stage_ids.len(), 0, "Stage 0 (from EmptyExec) should have no input stages. Stage: {:?}", stage0);
        assert_eq!(stage0.output_partitions, input_plan.output_partitioning().partition_count(), "Stage 0 (from EmptyExec) output_partitions. Stage: {:?}", stage0);
        assert!(stage0.stage_id.starts_with("stage_"), "Stage 0 ID format error: {}", stage0.stage_id);

        // Stage 1 (from RepartitionExec - this is the main stage for the query root)
        assert_eq!(stage1.input_stage_ids.len(), 1, "Stage 1 (from RepartitionExec) should have one input stage. Stage: {:?}", stage1);
        assert_eq!(stage1.input_stage_ids[0], stage0.stage_id, "Stage 1 should take input from Stage 0. Stage: {:?}", stage1);
        assert_eq!(stage1.output_partitions, num_output_partitions_repart, "Stage 1 (from RepartitionExec) output_partitions. Stage: {:?}", stage1);
        assert!(stage1.stage_id.starts_with("stage_"), "Stage 1 ID format error: {}", stage1.stage_id);
        assert_ne!(stage0.stage_id, stage1.stage_id, "Stage IDs must be unique");
    }

    #[test]
    fn test_plan_with_multiple_shuffles() {
        let planner = StagePlanner;

        // Plan: RepartitionExec_Top -> RepartitionExec_Bottom -> EmptyExec
        let empty_leaf_plan = an_empty_plan();
        let num_partitions_bottom = 4;
        let num_partitions_top = 2;

        let repart_bottom = Arc::new(
            RepartitionExec::try_new(
                empty_leaf_plan.clone(),
                Partitioning::RoundRobinBatch(num_partitions_bottom),
            )
            .unwrap(),
        );

        let plan = Arc::new(
            RepartitionExec::try_new(
                repart_bottom.clone(), // Input is the RepartitionExec_Bottom
                Partitioning::RoundRobinBatch(num_partitions_top),
            )
            .unwrap(),
        );

        let stages_result = planner.plan_stages(plan.clone());
        assert!(stages_result.is_ok(), "Planning failed: {:?}", stages_result.err());
        let stages = stages_result.unwrap();

        // Expected: Three stages.
        // Stage 0: from EmptyExec
        // Stage 1: from RepartitionExec_Bottom
        // Stage 2: from RepartitionExec_Top (final consuming stage)
        assert_eq!(stages.len(), 3, "Should create three stages. Stages: {:?}", stages);

        let stage0 = &stages[0]; // From EmptyExec
        let stage1 = &stages[1]; // From RepartitionExec_Bottom
        let stage2 = &stages[2]; // From RepartitionExec_Top

        // Stage 0 (from EmptyExec)
        assert_eq!(stage0.input_stage_ids.len(), 0, "Stage 0 inputs mismatch. Stage: {:?}", stage0); // Message updated
        assert_eq!(stage0.output_partitions, empty_leaf_plan.output_partitioning().partition_count(), "Stage 0 (EmptyExec) output_partitions. Stage: {:?}", stage0);
        assert!(stage0.stage_id.starts_with("stage_"), "Stage 0 ID format: {}", stage0.stage_id);

        // Stage 1 (from RepartitionExec_Bottom)
        assert_eq!(stage1.input_stage_ids.len(), 1, "Stage 1 (Repartition_Bottom) inputs. Stage: {:?}", stage1); // This line is preserved
        assert_eq!(stage1.input_stage_ids[0], stage0.stage_id, "Stage 1 should depend on Stage 0. Stage: {:?}", stage1); // Message updated
        assert_eq!(stage1.output_partitions, num_partitions_bottom, "Stage 1 (Repartition_Bottom) output_partitions. Stage: {:?}", stage1);
        assert!(stage1.stage_id.starts_with("stage_"), "Stage 1 ID format: {}", stage1.stage_id);

        // Stage 2 (from RepartitionExec_Top)
        assert_eq!(stage2.input_stage_ids.len(), 1, "Stage 2 (Repartition_Top) inputs. Stage: {:?}", stage2); // This line is preserved
        assert_eq!(stage2.input_stage_ids[0], stage1.stage_id, "Stage 2 should depend on Stage 1. Stage: {:?}", stage2); // Message updated
        assert_eq!(stage2.output_partitions, num_partitions_top, "Stage 2 (Repartition_Top) output_partitions. Stage: {:?}", stage2);
        assert!(stage2.stage_id.starts_with("stage_"), "Stage 2 ID format: {}", stage2.stage_id);

        assert_ne!(stage0.stage_id, stage1.stage_id, "Stage IDs must be unique (0 vs 1)");
        assert_ne!(stage1.stage_id, stage2.stage_id, "Stage IDs must be unique (1 vs 2)");
        assert_ne!(stage0.stage_id, stage2.stage_id, "Stage IDs must be unique (0 vs 2)");
    }
}
