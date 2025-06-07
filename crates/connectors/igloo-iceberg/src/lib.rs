use async_trait::async_trait;
use datafusion::arrow::datatypes::{SchemaRef, Field, DataType as ArrowDataType};
use datafusion::arrow::error::ArrowError;
use datafusion::config::ConfigOptions;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::TableProviderFilterPushDown; // Removed LogicalPlan, UserDefinedLogicalNode
use datafusion::physical_plan::parquet::ParquetExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PhysicalExpr, PlanProperties,
};
use datafusion::scalar::ScalarValue;
use iceberg_rust::arrow::schema_to_arrow;
use iceberg_rust::spec::types::StructField;
use iceberg_rust::table::Table;
use iceberg_rust::scan::Scan;
use object_store::ObjectStore;
use std::any::Any;
use std::sync::Arc;

// TODO: Will likely need to store a Catalog or specific table loading information
// depending on how table discovery/loading is meant to work with the Pluggable Catalog.
// For now, assuming the Table is loaded externally and passed to the provider.
pub struct IcebergTableProvider {
    table: Arc<Table>,
    // Storing object_store separately as ParquetExec needs Arc<dyn ObjectStore>
    // and Table.object_store() returns &dyn ObjectStore
    object_store: Arc<dyn ObjectStore>,
}

impl IcebergTableProvider {
    // Constructor that takes an Arc<Table> and Arc<dyn ObjectStore>
    pub fn new(table: Arc<Table>, object_store: Arc<dyn ObjectStore>) -> Self {
        IcebergTableProvider { table, object_store }
    }

    // Placeholder for a method that might load a table based on a name or path
    // This will depend on integration with the main catalog (Dev 1's work)
    // pub async fn load(table_name_or_path: &str, catalog: Arc<dyn YourCatalogTrait>) -> Result<Self, String> {
    //     // ... logic to load table using iceberg-rust ...
    //     // Err("Not implemented".to_string())
    // }
}

#[async_trait]
impl TableProvider for IcebergTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        // Convert Iceberg schema to DataFusion Arrow schema
        // Assuming current_schema(None) gives the latest schema.
        // The actual schema fetching might need to be more robust depending on snapshot handling.
        let iceberg_schema = self
            .table
            .current_schema(None)
            .expect("Failed to get current schema from Iceberg table"); // Consider error handling
        Arc::new(schema_to_arrow(&iceberg_schema).expect("Failed to convert Iceberg schema to Arrow schema"))
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        _filters: &[PhysicalExpr], // TODO: Pass filters to Iceberg scan
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // 1. Create an Iceberg scan
        let mut scan_builder = Scan::builder(self.table.clone());

        // Apply projection to the scan builder
        if let Some(projection_indices) = projection {
            let current_iceberg_schema = self.table.current_schema(None)
                .map_err(|e| DataFusionError::Internal(format!("Failed to get current schema for projection: {}", e)))?;

            // Get the field IDs from the schema based on projection indices
            let projected_field_ids: Vec<i32> = projection_indices.iter()
                .map(|idx| current_iceberg_schema.fields().get(*idx).map(|f| f.id))
                .collect::<Option<Vec<i32>>>()
                .ok_or_else(|| DataFusionError::Internal("Invalid projection index".to_string()))?;

            // Select fields by their IDs
            let projected_schema_fields = current_iceberg_schema.select_fields_by_id(&projected_field_ids);

            // Get the names of the projected fields
            let projected_field_names: Vec<String> = projected_schema_fields.iter()
                .map(|f: &Arc<StructField>| f.name.clone())
                .collect();

            if !projected_field_names.is_empty() {
                scan_builder = scan_builder.select(projected_field_names);
            }
        }

        let scan = scan_builder
            .build()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // 2. Get data files from the scan (these are paths to Parquet files)
        let files_iter = scan
            .files(state.task_ctx()) // Pass TaskContext from SessionState
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let mut file_groups: Vec<Vec<datafusion::datasource::listing::PartitionedFile>> = Vec::new();

        for file_scan_task_result in files_iter {
            let file_scan_task = file_scan_task_result.map_err(|e| DataFusionError::External(Box::new(e)))?;
            let data_file = file_scan_task.file;

            // TODO: Convert Iceberg partition values (Struct) to Vec<ScalarValue>
            // This requires knowing the partition spec and types.
            // For now, using empty partition_values.
            let partition_values = vec![];
            // let partition_values = data_file.partition().iter().map(|val| {
            //     // This is a placeholder. Actual conversion will depend on the type of `val`
            //     // which comes from `iceberg_rust::spec::values::Value`.
            //     // You'll need to match on the `Value` enum and convert to `ScalarValue`.
            //     // e.g., Value::Long(l) => ScalarValue::Int64(Some(*l)),
            //     // Value::String(s) => ScalarValue::Utf8(Some(s.clone())),
            //     ScalarValue::Null // Placeholder
            // }).collect::<Vec<ScalarValue>>();


            let partitioned_file = datafusion::datasource::listing::PartitionedFile {
                object_meta: object_store::ObjectMeta {
                    location: data_file.file_path.clone().into(),
                    last_modified: chrono::DateTime::from_timestamp_millis(data_file.file_update_time())
                        .unwrap_or_else(|| chrono::DateTime::from_timestamp(0,0).unwrap()), // Fallback for invalid timestamp
                    size: data_file.file_size_in_bytes as usize,
                    e_tag: None,
                    version: None,
                },
                partition_values,
                range: None,
                statistics: None, // TODO: Populate if possible from data_file statistics
                extensions: None,
            };
            file_groups.push(vec![partitioned_file]);
        }

        if file_groups.is_empty() {
            // No files to scan, return an empty plan with the correct schema
            return Ok(Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
                self.schema_for_projection(projection),
            )));
        }

        // 3. Create ParquetExec
        // The schema passed to ParquetExec should be the projected schema.
        let exec_schema = self.schema_for_projection(projection);

        let parquet_exec = ParquetExec::builder_with_plan_properties(
            file_groups,
            exec_schema.clone(),
            PlanProperties::new(
                datafusion::physical_plan::EquivalenceProperties::new(exec_schema.clone()),
                datafusion::physical_plan::Partitioning::UnknownPartitioning(1), // Assuming single partition for now
                datafusion::physical_plan::ExecutionMode::Bounded,
            ),
        )
        .with_limit(limit)
        // TODO: Wire predicate pushdown to ParquetExec if not handled by Iceberg scan
        // .with_predicate(predicate) // A  DataFusion PhysicalExpr
        .build_arc_with_object_store(self.object_store.clone());

        Ok(parquet_exec)
    }

    fn supports_filters_pushdown(
        &self,
        _filters: &[&datafusion::logical_expr::Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        // For now, claim all filters can be pushed down.
        // Iceberg scan will attempt partition pruning based on its own filter translation.
        // ParquetExec can also perform predicate pushdown into Parquet row groups.
        // A more accurate implementation would inspect the filters and see which ones
        // can be translated to Iceberg expressions and which can be handled by Parquet.
        // If a filter can be fully handled by Iceberg, mark as Exact.
        // If it can be partially handled (e.g. by Parquet but not full partition pruning), mark as Inexact.
        // If it cannot be handled at all by the provider, mark as Unsupported.
        Ok(vec![TableProviderFilterPushDown::Inexact; _filters.len()])
    }
}

impl IcebergTableProvider {
    // Helper to get the schema, possibly projected
    fn schema_for_projection(&self, projection: Option<&Vec<usize>>) -> SchemaRef {
        let base_schema = self.schema(); // This is already an Arc<Schema>
        match projection {
            Some(indices) => {
                let projected_fields: Vec<FieldRef> = indices
                    .iter()
                    .map(|i| base_schema.field(*i).clone()) // Clones Arc<Field>
                    .collect();
                Arc::new(datafusion::arrow::datatypes::Schema::new(projected_fields))
            }
            None => base_schema,
        }
    }
}

// Helper type alias for Arc<Field>
type FieldRef = Arc<datafusion::arrow::datatypes::Field>;

// TODO: Add DisplayAs trait implementation for ExecutionPlanProperties if needed by newer DataFusion
// impl DisplayAs for PlanProperties {
//     fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
//         write!(f, "PlanProperties(..)") // Simplified display
//     }
// }

// Removed UserDefinedLogicalNode implementation
