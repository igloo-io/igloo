use std::fs::File;
// use std::path::Path; // Commented out as unused
// use std::sync::Arc; // Commented out as unused

use crate::{DataType, Field, Schema, TableProvider, Row, Result}; // Use crate:: for types from lib.rs
use igloo_common::error::Error; // Assuming igloo_common::Error is the error type

use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::reader::RowIter; // This is from parquet::record::RowIter, might need FQ path if ambiguous
use parquet::record::Field as ParquetSdkField; // Alias to avoid confusion with our Field
use parquet::schema::types::Type as ParquetType;

pub struct ParquetTable {
    path: String,
}

impl ParquetTable {
    pub fn new(path: &str) -> Self {
        Self { path: path.to_string() }
    }

    // Helper function to convert Parquet physical types to our DataType
    fn parquet_type_to_datatype(parquet_type: &ParquetType) -> DataType {
        match parquet_type.get_physical_type() {
            parquet::basic::Type::BOOLEAN => DataType::Boolean,
            parquet::basic::Type::INT32 => DataType::Int64, // Promote to Int64 for simplicity
            parquet::basic::Type::INT64 => DataType::Int64,
            // INT96 was historically used for timestamps. Representing as String for now.
            // Modern Parquet uses INT64 with logical type TIMESTAMP_MILLIS/MICROS.
            parquet::basic::Type::INT96 => DataType::String,
            parquet::basic::Type::FLOAT => DataType::Float64, // Promote to Float64
            parquet::basic::Type::DOUBLE => DataType::Float64,
            parquet::basic::Type::BYTE_ARRAY | parquet::basic::Type::FIXED_LEN_BYTE_ARRAY => DataType::String,
            // If other physical types are encountered, default to String.
            // _ => DataType::String, // Catch-all, consider if this is too broad or if specific errors are better
        }
    }
}

impl TableProvider for ParquetTable {
    fn schema(&self) -> Result<Schema> {
        let file = File::open(&self.path).map_err(|e| Error::new(&format!("Failed to open Parquet file {}: {}", self.path, e)))?;
        let reader = SerializedFileReader::new(file).map_err(|e| Error::new(&format!("Failed to create Parquet reader for {}: {}", self.path, e)))?;
        let metadata = reader.metadata();
        let file_schema = metadata.file_metadata().schema_descr();

        let mut fields = Vec::new();
        for i in 0..file_schema.num_columns() {
            let column_descr = file_schema.column(i);
            let pq_type = column_descr.self_type(); // Get the ParquetType (which is Arc<ParquetType>)

            // Determine nullability: In Parquet, OPTIONAL means nullable. REQUIRED means not nullable.
            // self_type_def() gives access to the TypeDefinition which includes repetition.
            let is_optional = pq_type.get_basic_info().repetition() == parquet::basic::Repetition::OPTIONAL;

            // Handle actual ParquetType, not just its physical mapping for schema representation if possible
            // For now, the logic relies on physical type and then defaults complex types to String.
            match pq_type { // pq_type is already &Type
                ParquetType::PrimitiveType { .. } => {
                     fields.push(Field {
                        name: column_descr.name().to_string(),
                        data_type: Self::parquet_type_to_datatype(pq_type),
                        nullable: is_optional,
                    });
                }
                // For complex types (groups, lists, maps), default to String as a placeholder.
                // A more robust implementation would inspect logical types or recurse into groups.
                _group_type => {
                     fields.push(Field {
                        name: column_descr.name().to_string(),
                        data_type: DataType::String, // Placeholder for complex types
                        nullable: is_optional, // Group nullability depends on its definition
                    });
                }
            }
        }
        Ok(Schema { fields })
    }

    fn scan(&self) -> Result<Box<dyn Iterator<Item = Row>>> {
        let file = File::open(&self.path).map_err(|e| Error::new(&format!("Failed to open Parquet file {}: {}", self.path, e)))?;
        // Using Arc for the reader as it might be needed by RowIter or other parts of the API
        // if they expect shareable readers or if the lifetime needs to extend.
        // SerializedFileReader itself is not cloneable directly for Arc if not wrapped.
        // However, RowIter::from_file takes a &R where R: FileReader.
        // Let's try with direct reader first. If lifetimes/ownership becomes an issue, reconsider Arc.
        let reader = SerializedFileReader::new(file).map_err(|e| Error::new(&format!("Failed to create Parquet reader for {}: {}", self.path, e)))?;

        // We need to specify the schema for projection. None means all columns.
        // The schema here refers to the schema of the records we want to read (projection).
        // If None, it typically defaults to the file schema.
        let row_iter = RowIter::from_file(None, &reader).map_err(|e| Error::new(&format!("Failed to create Parquet row iterator: {}", e)))?;

        let mut rows_vec: Vec<Row> = Vec::new();
        for record_result in row_iter {
            let record = record_result.map_err(|e| Error::new(&format!("Failed to read Parquet record: {}", e)))?;
            let mut current_row: Row = Vec::new();
            // The record is a Row as defined by parquet crate, which is a Vec<(String, ParquetSdkField)> effectively
            for (_name, field_value) in record.get_column_iter() {
                let string_value = match field_value {
                    ParquetSdkField::Null => "".to_string(), // Represent null as empty string
                    ParquetSdkField::Bool(b) => b.to_string(),
                    ParquetSdkField::Byte(b) => b.to_string(), // parquet::record::Field::Byte(i8)
                    ParquetSdkField::Short(s) => s.to_string(), // parquet::record::Field::Short(i16)
                    ParquetSdkField::Int(i) => i.to_string(),   // parquet::record::Field::Int(i32)
                    ParquetSdkField::Long(l) => l.to_string(), // parquet::record::Field::Long(i64)
                    ParquetSdkField::Float(f) => f.to_string(),
                    ParquetSdkField::Double(d) => d.to_string(),
                    ParquetSdkField::Str(s) => s.clone(),
                    ParquetSdkField::Bytes(b) => String::from_utf8_lossy(b.data()).into_owned(), // .data() returns &[u8]
                    ParquetSdkField::Date(d) => d.to_string(), // Assuming d is u32 representing days
                    ParquetSdkField::TimestampMillis(ts) => ts.to_string(),
                    ParquetSdkField::TimestampMicros(ts) => ts.to_string(),
                    // TODO: Handle other ParquetSdkField variants: Decimal, Group, List, Map
                    // ParquetSdkField::Decimal
                    // ParquetSdkField::Group
                    // ParquetSdkField::ListInternal
                    // ParquetSdkField::MapInternal
                    _ => format!("Unsupported_Type_{:?}", field_value), // Fallback for unhandled types
                };
                current_row.push(string_value);
            }
            rows_vec.push(current_row);
        }

        Ok(Box::new(rows_vec.into_iter()))
    }
}

#[cfg(test)]
mod tests {
    use super::*; // Imports ParquetTable, etc.
    use crate::{DataType, Field, Schema}; // Imports Schema, Field, DataType from lib.rs

    // Helper to construct the expected schema for test_data.parquet
    fn get_expected_schema() -> Schema {
        Schema {
            fields: vec![
                Field { name: "id".to_string(), data_type: DataType::Int64, nullable: true },
                Field { name: "value".to_string(), data_type: DataType::String, nullable: true },
                // Note: Nullability might need adjustment based on actual Parquet file metadata.
                // The test data itself doesn't have nulls, but the Parquet schema can define nullability.
                // For now, assuming true for simplicity, as typically Parquet columns are nullable by default.
            ],
        }
    }

    #[test]
    fn test_parquet_table_schema() {
        // Ensure test_data.parquet is in crates/connectors/filesystem/test_data.parquet
        // The subtask for creating it should have placed it there.
        let parquet_table = ParquetTable::new("test_data.parquet"); // Relative to Cargo.toml of the crate
        let schema_result = parquet_table.schema();

        assert!(schema_result.is_ok(), "schema() returned an error: {:?}", schema_result.err());
        let actual_schema = schema_result.unwrap();
        let expected_schema = get_expected_schema();

        assert_eq!(actual_schema.fields.len(), expected_schema.fields.len(), "Schema field count mismatch");

        for (actual_field, expected_field) in actual_schema.fields.iter().zip(expected_schema.fields.iter()) {
            assert_eq!(actual_field.name, expected_field.name, "Field name mismatch");
            assert_eq!(actual_field.data_type, expected_field.data_type, "Field data_type mismatch for field {}", actual_field.name);
            // Nullability check can be tricky if not perfectly inferred; focus on name and type first.
            assert_eq!(actual_field.nullable, expected_field.nullable, "Field nullable mismatch for field {}", actual_field.name);
        }
    }

    #[test]
    fn test_parquet_table_scan() {
        let parquet_table = ParquetTable::new("test_data.parquet");
        let scan_result = parquet_table.scan();

        assert!(scan_result.is_ok(), "scan() returned an error: {:?}", scan_result.err());
        let rows_iter = scan_result.unwrap();
        let rows: Vec<Row> = rows_iter.collect();

        assert_eq!(rows.len(), 3, "Scan returned incorrect number of rows");

        let expected_rows: Vec<Row> = vec![
            vec!["1".to_string(), "foo".to_string()],
            vec!["2".to_string(), "bar".to_string()],
            vec!["3".to_string(), "baz".to_string()],
        ];

        for (i, actual_row) in rows.iter().enumerate() {
            assert_eq!(actual_row, &expected_rows[i], "Row {} content mismatch", i);
        }
    }

    #[test]
    fn test_parquet_table_file_not_found_schema() {
        let parquet_table = ParquetTable::new("non_existent_file.parquet");
        let schema_result = parquet_table.schema();
        assert!(schema_result.is_err(), "schema() should fail for non-existent file");
    }

    #[test]
    fn test_parquet_table_file_not_found_scan() {
        let parquet_table = ParquetTable::new("non_existent_file.parquet");
        let scan_result = parquet_table.scan();
        assert!(scan_result.is_err(), "scan() should fail for non-existent file");
    }
}
