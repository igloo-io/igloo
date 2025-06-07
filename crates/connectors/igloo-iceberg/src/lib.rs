use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema, SchemaRef};
use datafusion::arrow::error::ArrowError;
use datafusion::datasource::listing::PartitionedFile; // Added based on scan() usage
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::physical_plan::parquet::ParquetExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PhysicalExpr, PlanProperties,
};
// datafusion::scalar::ScalarValue was used in commented out code, removing for now. If needed, re-add.
// iceberg_rust::spec::types::StructField was used in scan for projection, will be aliased or used fully qualified.
use iceberg_rust::arrow::schema_to_arrow;
use iceberg_rust::spec::schema::Schema as IcebergSchema; // Alias to avoid conflict
use iceberg_rust::spec::types::StructField as IcebergStructField; // Alias for clarity
use iceberg_rust::table::Table;
use iceberg_rust::Scan;
use object_store::{ObjectMeta, ObjectStore}; // Added ObjectMeta
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
        match self.table.current_schema(None) {
            Ok(iceberg_data_schema) => { // Renamed to avoid conflict with IcebergSchema alias
                match schema_to_arrow(&iceberg_data_schema) {
                    Ok(arrow_schema) => Arc::new(arrow_schema),
                    Err(e) => {
                        // Log error or handle appropriately
                        // For now, panic as SchemaRef must be returned
                        // TODO: Consider logging this panic or providing a more graceful error.
                        panic!("Failed to convert Iceberg schema to Arrow schema: {}", e);
                    }
                }
            }
            Err(e) => {
                // Log error or handle appropriately
                // TODO: Consider logging this panic or providing a more graceful error.
                panic!("Failed to get current schema from Iceberg table: {}", e);
            }
        }
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
            let current_iceberg_data_schema = self.table.current_schema(None) // Renamed to avoid conflict
                .map_err(|e| DataFusionError::Internal(format!("Failed to get current schema for projection: {}", e)))?;

            let projected_field_ids: Vec<i32> = projection_indices.iter()
                .map(|idx| current_iceberg_data_schema.fields().get(*idx).map(|f| f.id))
                .collect::<Option<Vec<i32>>>()
                .ok_or_else(|| DataFusionError::Internal("Invalid projection index".to_string()))?;

            let projected_schema_fields = current_iceberg_data_schema.select_fields_by_id(&projected_field_ids);

            let projected_field_names: Vec<String> = projected_schema_fields.iter()
                .map(|f: &Arc<IcebergStructField>| f.name.clone()) // Used aliased IcebergStructField
                .collect();

            if !projected_field_names.is_empty() {
                scan_builder = scan_builder.select(projected_field_names);
            }
        }

        let scan = scan_builder
            .build()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let files_iter = scan
            .files(state.task_ctx())
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let mut file_groups: Vec<Vec<PartitionedFile>> = Vec::new(); // Used imported PartitionedFile

        for file_scan_task_result in files_iter {
            let file_scan_task = file_scan_task_result.map_err(|e| DataFusionError::External(Box::new(e)))?;
            let data_file = file_scan_task.file;

            let partition_values = vec![]; // TODO: Convert Iceberg partition values (Struct) to Vec<ScalarValue>

            let partitioned_file = PartitionedFile { // Used imported PartitionedFile
                object_meta: ObjectMeta { // Used imported ObjectMeta
                    location: data_file.file_path.clone().into(),
                    last_modified: chrono::DateTime::from_timestamp_millis(data_file.file_update_time())
                        .unwrap_or_else(|| chrono::DateTime::from_timestamp(0,0).unwrap()),
                    size: data_file.file_size_in_bytes as usize,
                    e_tag: None,
                    version: None,
                },
                partition_values,
                range: None,
                statistics: None,
                extensions: None,
            };
            file_groups.push(vec![partitioned_file]);
        }

        if file_groups.is_empty() {
            return Ok(Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
                self.schema_for_projection(projection),
            )));
        }

        let exec_schema = self.schema_for_projection(projection);

        let parquet_exec = ParquetExec::builder_with_plan_properties(
            file_groups,
            exec_schema.clone(),
            PlanProperties::new(
                datafusion::physical_plan::EquivalenceProperties::new(exec_schema.clone()),
                datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
                datafusion::physical_plan::ExecutionMode::Bounded,
            ),
        )
        .with_limit(limit)
        .build_arc_with_object_store(self.object_store.clone());

        Ok(parquet_exec)
    }

    fn supports_filters_pushdown(
        &self,
        _filters: &[&datafusion::logical_expr::Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; _filters.len()])
    }
}

impl IcebergTableProvider {
    // Helper to get the schema, possibly projected
    fn schema_for_projection(&self, projection: Option<&Vec<usize>>) -> SchemaRef {
        let base_schema = self.schema();
        match projection {
            Some(indices) => {
                let projected_fields: Vec<FieldRef> = indices
                    .iter()
                    .map(|i| base_schema.field(*i).clone())
                    .collect();
                Arc::new(ArrowSchema::new(projected_fields)) // Used aliased ArrowSchema
            }
            None => base_schema,
        }
    }
}

// Helper type alias for Arc<Field>
type FieldRef = Arc<Field>; // Field is already imported as datafusion::arrow::datatypes::Field
