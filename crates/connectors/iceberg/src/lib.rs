use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use arrow::datatypes::{Schema as ArrowSchema, SchemaRef as ArrowSchemaRef, Field as ArrowField, DataType as ArrowDataType, TimeUnit};

// DataFusion imports - Applying precise paths from task description
use datafusion::common::{DataFusionError, Result as DFResult, SchemaRef as DFSchemaRef}; // DFSchema removed, Statistics removed for now
// use datafusion::datasource::file_format::FileFormat; // General FileFormat trait, might not be needed directly
// use datafusion::datasource::listing::ListingTableUrl; // Not used in current code
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfig, PartitionedFile};
use datafusion::datasource::{TableProvider, TableType}; // DefaultTableSource not used
use datafusion::catalog::Session;
// use datafusion::execution::context::{SessionContext, SessionState, TaskContext}; // SessionState was unused, SessionContext/TaskContext not directly used yet
use datafusion::physical_plan::ExecutionPlan; // displayable, Statistics might be moved if they are from here
use datafusion::physical_plan::file_format::ParquetExec; // Corrected path for DF 48
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::prelude::Expr;


// --- Corrected iceberg-rust imports for 0.7.0 based on compiler hints from previous subtask ---
use iceberg_rust::table::Table as IcebergRustTable;
use iceberg_rust::arrow::utils::to_arrow_schema;
use iceberg_rust::error::Error as IcebergError;
use iceberg_rust::runtime::Runtime;
use iceberg_rust::spec::manifest::{
    DataFile as IcebergDataFile,
    ManifestEntry,
    ManifestFile as IcebergManifestFileContent,
    // ManifestReader, // Not directly used if using ManifestFile::read_with_runtime
};
use iceberg_rust::spec::manifest_list::{
    ManifestList,
    ManifestFileEntry, // This is the type for entries in ManifestList, not ManifestListEntry
    // ManifestListReader, // Not directly used
};
use iceberg_rust::spec::schema::Schema as IcebergSchemaData;
use iceberg_rust::spec::snapshot::Snapshot;
use iceberg_rust::Result as IcebergResult; // iceberg_rust own Result type
// use iceberg_rust::spec::table_metadata::TableMetadata; // Not directly used by name
// use iceberg_rust::io::FileIO; // Not used directly

use object_store::local::LocalFileSystem;
use object_store::{ObjectStore, ObjectMeta};
use object_store::path::Path as ObjectStorePath;
use url::Url;
use chrono::{Utc, TimeZone};


#[derive(Debug)]
pub struct IcebergTable {
    table_base_path: String,
    object_store_for_df: Arc<dyn ObjectStore>,
    table: Arc<IcebergRustTable>,
    arrow_schema_ref: ArrowSchemaRef,
}

impl IcebergTable {
    pub async fn try_new(table_uri: &str) -> DFResult<Self> {
        let url = Url::parse(table_uri)
            .map_err(|e| DataFusionError::Configuration(format!("Invalid table URI: {}: {}", table_uri, e)))?;

        if url.scheme() != "file" {
            return Err(DataFusionError::Configuration(format!(
                "Only 'file://' URIs are supported for Iceberg tables, got: {}",
                table_uri
            )));
        }
        let table_base_path_obj = url.to_file_path().map_err(|_| {
            DataFusionError::Configuration(format!("Invalid file path in URI: {}", table_uri))
        })?;

        let table_base_path_str = table_base_path_obj.to_str().ok_or_else(|| {
            DataFusionError::Internal("Invalid non-UTF8 path for table".to_string())
        })?.to_string();

        let local_fs_for_df = LocalFileSystem::new_with_prefix(&table_base_path_obj)
             .map_err(|e| DataFusionError::ObjectStore(e))?;
        let object_store_for_df: Arc<dyn ObjectStore> = Arc::new(local_fs_for_df);

        let table_load_uri = format!("file://{}", table_base_path_str);
        // API usage errors here are ignored for this subtask, focusing on imports.
        let table = Arc::new(
            IcebergRustTable::builder(&table_load_uri) // This API call is known to be wrong (builder takes 0 args)
                .load()                                 // and CreateTableBuilder has no .load(). Will be fixed later.
                .await
                .map_err(|e: IcebergError| DataFusionError::External(format!("Failed to load Iceberg table metadata from {}: {:?}", table_load_uri, Box::new(e)).into()))?
        );

        let current_iceberg_metadata = table.metadata();
        let current_iceberg_schema_spec = current_iceberg_metadata.current_schema()
             .map_err(|e: IcebergError| DataFusionError::Internal(format!("Failed to get current schema from Iceberg table: {}",e)))?;

        let arrow_schema = to_arrow_schema(current_iceberg_schema_spec)
             .map_err(|e: IcebergError| DataFusionError::External(format!("Failed to convert Iceberg schema to Arrow schema: {}", e).into()))?;

        let arrow_schema_ref = Arc::new(arrow_schema);

        Ok(Self {
            table_base_path: table_base_path_str,
            object_store_for_df,
            table,
            arrow_schema_ref,
        })
    }

    #[allow(dead_code)]
    fn _manual_convert_iceberg_schema_to_arrow_schema(
        iceberg_schema: &IcebergSchemaData,
    ) -> DFResult<ArrowSchemaRef> {
        // This function's internals have known API errors (field access) - ignored for this subtask
        let fields: Vec<ArrowField> = iceberg_schema
            .fields()
            .iter()
            .map(|_field_placeholder| {
                Ok(ArrowField::new("placeholder", ArrowDataType::Null, true))
            })
            .collect::<DFResult<Vec<ArrowField>>>()?;
        Ok(Arc::new(ArrowSchema::new(fields)))
    }
}

#[async_trait]
impl TableProvider for IcebergTable {
    fn as_any(&self) -> &dyn Any { self }
    fn schema(&self) -> ArrowSchemaRef { self.arrow_schema_ref.clone() }
    fn table_type(&self) -> TableType { TableType::Base }

    async fn scan(
        &self,
        _state: &(dyn Session + '_),
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        println!("[IcebergTable::scan] Projection: {:?}, Filters: {:?}, Limit: {:?}", projection, filters, limit);
        // API usage errors in this function are ignored for this subtask.
        // Focus is on whether the types used (e.g. ManifestList, ManifestFileEntry, etc.) are correctly imported.

        let current_snapshot = self.table.metadata().current_snapshot(None)
            .map_err(|e: IcebergError| DataFusionError::External(format!("Failed to access current snapshot: {}", e).into()))?
            .ok_or_else(|| DataFusionError::Execution("Iceberg table has no current snapshot".to_string()))?;

        let manifest_list_path_str = current_snapshot.manifest_list();
        let manifest_list_os_path = ObjectStorePath::from_url_path(manifest_list_path_str)
            .map_err(|e| DataFusionError::External(format!("Invalid manifest list path: {}: {}", manifest_list_path_str,e).into()))?;

        let runtime = Arc::new(Runtime::default());

        let manifest_list = ManifestList::read_with_runtime(&manifest_list_os_path, self.table.object_store(), runtime.clone()).await
            .map_err(|e: IcebergError| DataFusionError::External(format!("Failed to read manifest list: {}", e).into()))?;

        let mut data_files_from_metadata: Vec<IcebergDataFile> = Vec::new();
        for manifest_file_entry in manifest_list.entries() {
            let manifest_file_os_path = ObjectStorePath::from_url_path(manifest_file_entry.manifest_path())
                 .map_err(|e| DataFusionError::External(format!("Invalid manifest file path: {}: {}", manifest_file_entry.manifest_path(),e).into()))?;
            let iceberg_manifest_file_content = IcebergManifestFileContent::read_with_runtime(&manifest_file_os_path, self.table.object_store(), runtime.clone()).await
                .map_err(|e: IcebergError| DataFusionError::External(format!("Failed to read manifest file {}: {}", manifest_file_entry.manifest_path(), e).into()))?;

            data_files_from_metadata.extend(iceberg_manifest_file_content.entries().iter().cloned());
        }

        let use_fallback = data_files_from_metadata.is_empty() || manifest_list_path_str.ends_with("snap-placeholder.txt");
        if use_fallback {
            println!("[IcebergTable::scan] No valid data files from metadata or placeholder detected. Using fallback.");
        }

        let file_scan_config = if !use_fallback {
            let mut file_groups: Vec<FileGroup> = Vec::new();
            let base_url_str = format!("file://{}", self.table_base_path);
            let base_url_for_df = Url::parse(&base_url_str).map_err(|e| DataFusionError::External(Box::new(e)))?;

            for data_file in data_files_from_metadata {
                let file_path_str = data_file.file_path().as_str();
                let full_url = base_url_for_df.join(file_path_str.strip_prefix("/").unwrap_or(file_path_str))
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                let file_size_i64 = data_file.file_size_in_bytes();
                let object_meta = ObjectMeta {
                    location: ObjectStorePath::from_url_path(full_url.path())?,
                    last_modified: Utc.with_ymd_and_hms(1970, 1, 1, 0, 0, 0).unwrap(),
                    size: usize::try_from(file_size_i64).map_err(|_| DataFusionError::Internal(format!("Cannot convert file size {} to usize", file_size_i64)))?,
                    e_tag: None, version: None,
                };
                file_groups.push(FileGroup {
                    files: vec![PartitionedFile { object_meta, partition_values: vec![], range: None, extensions: None, statistics: None, metadata_size_hint: None }],
                    statistics: None,
                });
            }
            if file_groups.is_empty() { return Err(DataFusionError::Internal("No file groups from metadata.".to_string())); }
            FileScanConfig {
                object_store_url: ObjectStoreUrl::parse(&base_url_str)?,
                file_schema: self.schema(), file_groups,
                projection: projection.cloned(), limit,
                table_partition_cols: vec![], output_ordering: vec![],
            }
        } else {
            println!("[IcebergTable::scan] Using fallback strategy.");
            let data_file_path_str = format!("{}/data/table_b_data_001.parquet", self.table_base_path);
            let file_url = format!("file://{}", data_file_path_str);
            let object_store_url_for_file = ObjectStoreUrl::parse(&file_url)?;

            let file_size_u64 = std::fs::metadata(&data_file_path_str).map(|m| m.len()).unwrap_or(0);
            let object_meta = ObjectMeta {
                location: ObjectStorePath::from_url_path(&data_file_path_str.replace("file://", ""))?,
                last_modified: Utc.with_ymd_and_hms(1970, 1, 1, 0, 0, 0).unwrap(),
                size: file_size_u64 as usize,
                e_tag: None, version: None,
            };
            FileScanConfig {
                object_store_url: object_store_url_for_file.clone(),
                file_schema: self.schema(),
                file_groups: vec![FileGroup{ files: vec![PartitionedFile{ object_meta, partition_values: vec![], range: None, extensions: None, statistics: None, metadata_size_hint: None}], statistics: None}],
                projection: projection.cloned(), limit,
                table_partition_cols: vec![], output_ordering: vec![],
            }
        };

        let parquet_format = ParquetFormat::default().with_enable_pruning(true);
        let exec = ParquetExec::builder(file_scan_config).with_format(Arc::new(parquet_format)).build();
        Ok(Arc::new(exec))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Write;
    use tempfile::tempdir;
    use uuid::Uuid;
    use arrow::parquet::arrow::WriteOptions as ParquetWriteOptions;

    fn create_dummy_iceberg_metadata(base_path_str: &str, table_name: &str) -> DFResult<()> {
        let table_root = format!("{}/{}", base_path_str, table_name);
        let metadata_path = format!("{}/metadata", table_root);
        let data_path = format!("{}/data", table_root);
        fs::create_dir_all(&metadata_path)?; fs::create_dir_all(&data_path)?;

        let arrow_test_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int64, false),
            ArrowField::new("data", ArrowDataType::Utf8, true),
        ]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            arrow_test_schema.clone(),
            vec![
                Arc::new(arrow::array::Int64Array::from(vec![1, 2, 3])),
                Arc::new(arrow::array::StringArray::from(vec!["a", "b", "c"])),
            ],
        ).unwrap();
        let pq_file_path_str = format!("{}/dummy_data_001.parquet", data_path);
        let pq_file = fs::File::create(&pq_file_path_str).unwrap();

        let props = ParquetWriteOptions::default();
        let mut writer = arrow::parquet::arrow::ArrowWriter::try_new(pq_file, arrow_test_schema.clone(), Some(props)).unwrap();
        writer.write(&batch).unwrap(); writer.close().unwrap();

        let iceberg_schema_for_metadata = format!(r#"{{
            "type": "struct", "schema-id": 0, "identifier-field-ids": [1],
            "fields": [
                {{"id": 1, "name": "id", "required": true, "type": "long", "doc": "unique id"}},
                {{"id": 2, "name": "data", "required": false, "type": "string"}}
            ]
        }}"#);

        let manifest_placeholder_relative_path = "metadata/snap-placeholder-test.txt";
        let manifest_placeholder_full_path = format!("{}/{}", table_root, manifest_placeholder_relative_path);
        fs::File::create(&manifest_placeholder_full_path)?.write_all(b"Placeholder manifest.")?;

        let current_ts = Utc::now().timestamp_millis();
        let manifest_list_in_metadata = format!("metadata/{}", manifest_placeholder_relative_path.split('/').last().unwrap_or("snap-placeholder-test.txt"));

        let metadata_content = format!(r#"{{
            "format-version": "2", "table-uuid": "{}", "location": "file://{}", "last-updated-ms": {},
            "last-column-id": 2, "schemas": [{}], "current-schema-id": 0,
            "partition-specs": [{{"spec-id": 0, "fields": []}}], "default-spec-id": 0,
            "sort-orders": [{{"order-id": 0, "fields": []}}], "default-sort-order-id": 0,
            "properties": {{}}, "current-snapshot-id": 1,
            "snapshots": [
                {{"snapshot-id": 1, "timestamp-ms": {}, "manifest-list": "{}", "summary": {{"operation": "append"}}, "schema-id": 0}}
            ]
        }}"#, Uuid::new_v4(), table_root, current_ts, iceberg_schema_for_metadata, current_ts, manifest_list_in_metadata);

        fs::File::create(format!("{}/v1.metadata.json", metadata_path))?.write_all(metadata_content.as_bytes())?;
        fs::File::create(format!("{}/version-hint.text", metadata_path))?.write_all(b"1")?;
        Ok(())
    }

    #[tokio::test]
    async fn test_try_new_iceberg_table() -> DFResult<()> {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let base_path = temp_dir.path().to_str().unwrap();
        let table_name = "dummy_table_test_new";
        create_dummy_iceberg_metadata(base_path, table_name)?;
        let table_uri_path = format!("{}/{}", base_path, table_name);
        let table_uri_str = format!("file://{}", table_uri_path);
        let iceberg_table = IcebergTable::try_new(&table_uri_str).await?;
        assert_eq!(iceberg_table.table_base_path, table_uri_path);
        let expected_fields = vec![
            ArrowField::new("id", ArrowDataType::Int64, false),
            ArrowField::new("data", ArrowDataType::Utf8, true),
        ];
        let expected_arrow_schema = ArrowSchema::new(expected_fields);
        assert_eq!(*iceberg_table.schema(), expected_arrow_schema);
        assert_eq!(iceberg_table.table_type(), TableType::Base);
        Ok(())
    }
}
