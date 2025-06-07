// This file defines the structures related to query stages in the coordinator.

pub struct QueryStage {
    pub stage_id: String,
    pub plan_fragment: Vec<u8>,
    pub input_stage_ids: Vec<String>,
    pub output_partitions: usize,
}
