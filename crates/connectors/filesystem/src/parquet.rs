use std::fs::File;
// std::path::Path may not be needed if path strings are used directly
// use std::path::Path;
// std::sync::Arc may not be needed for this implementation
// use std::sync::Arc;

use crate::{DataType, Field, Schema, TableProvider, Row, Result}; // Use crate:: for types from lib.rs
use igloo_common::error::Error;

use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::reader::RowIter; // Ensure this is the correct RowIter if multiple exist
use parquet::record::Field as ParquetSdkField;
use parquet::schema::types::Type as ParquetType;
use parquet::basic::Repetition;


pub struct ParquetTable {
    path: String,
}

impl ParquetTable {
    pub fn new(path: &str) -> Self {
        Self { path: path.to_string() }
    }

    fn parquet_type_to_datatype(parquet_type: &ParquetType) -> DataType {
        match parquet_type.get_physical_type() {
            parquet::basic::Type::BOOLEAN => DataType::Boolean,
            parquet::basic::Type::INT32 => DataType::Int64,
            parquet::basic::Type::INT64 => DataType::Int64,
            parquet::basic::Type::INT96 => DataType::String,
            parquet::basic::Type::FLOAT => DataType::Float64,
            parquet::basic::Type::DOUBLE => DataType::Float64,
            parquet::basic::Type::BYTE_ARRAY | parquet::basic::Type::FIXED_LEN_BYTE_ARRAY => DataType::String,
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
            let pq_type = column_descr.self_type();

            // Determine nullability based on repetition
            let is_nullable = pq_type.get_basic_info().repetition() == Repetition::OPTIONAL;

            // Handle only primitive types for now, others default to String
            let data_type = if pq_type.is_primitive() {
                Self::parquet_type_to_datatype(pq_type)
            } else {
                DataType::String // Placeholder for complex types like groups, lists, maps
            };

            fields.push(Field {
                name: column_descr.name().to_string(),
                data_type: data_type,
                nullable: is_nullable,
            });
        }
        Ok(Schema { fields })
    }

    fn scan(&self) -> Result<Box<dyn Iterator<Item = Row>>> {
        let file = File::open(&self.path).map_err(|e| Error::new(&format!("Failed to open Parquet file {}: {}", self.path, e)))?;
        let reader = SerializedFileReader::new(file).map_err(|e| Error::new(&format!("Failed to create Parquet reader for {}: {}", self.path, e)))?;

        let row_iter = RowIter::from_file(None, &reader).map_err(|e| Error::new(&format!("Failed to create Parquet row iterator: {}", e)))?;

        let mut rows_vec: Vec<Row> = Vec::new();
        for record_result in row_iter {
            let record = record_result.map_err(|e| Error::new(&format!("Failed to read Parquet record: {}", e)))?;
            let mut current_row: Row = Vec::new();
            for (_name, field_value) in record.get_column_iter() {
                let string_value = match field_value {
                    ParquetSdkField::Null => "".to_string(),
                    ParquetSdkField::Bool(b) => b.to_string(),
                    ParquetSdkField::Byte(b) => b.to_string(),
                    ParquetSdkField::Short(s) => s.to_string(),
                    ParquetSdkField::Int(i) => i.to_string(),
                    ParquetSdkField::Long(l) => l.to_string(),
                    ParquetSdkField::Float(f) => f.to_string(),
                    ParquetSdkField::Double(d) => d.to_string(),
                    ParquetSdkField::Str(s) => s.clone(),
                    ParquetSdkField::Bytes(b) => String::from_utf8_lossy(b.data()).to_string(),
                    ParquetSdkField::Date(d) => d.to_string(), // Number of days from Unix epoch
                    ParquetSdkField::TimestampMillis(ts) => ts.to_string(),
                    ParquetSdkField::TimestampMicros(ts) => ts.to_string(),
                    // Add other ParquetSdkField variants as needed for full coverage
                    _ => format!("Unsupported_Field_Type_{:?}", field_value),
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
    use super::*;
    use crate::{DataType, Field, Schema, Row}; // Ensure Row is also imported from crate

    fn get_expected_schema() -> Schema {
        Schema {
            fields: vec![
                Field { name: "id".to_string(), data_type: DataType::Int64, nullable: true },
                Field { name: "value".to_string(), data_type: DataType::String, nullable: true },
            ],
        }
    }

    #[test]
    fn test_parquet_table_schema() {
        let parquet_table = ParquetTable::new("test_data.parquet");
        let schema_result = parquet_table.schema();

        assert!(schema_result.is_ok(), "schema() returned an error: {:?}", schema_result.err());
        let actual_schema = schema_result.unwrap();
        let expected_schema = get_expected_schema();

        assert_eq!(actual_schema.fields.len(), expected_schema.fields.len(), "Schema field count mismatch");

        for (actual_field, expected_field) in actual_schema.fields.iter().zip(expected_schema.fields.iter()) {
            assert_eq!(actual_field.name, expected_field.name, "Field name mismatch for field {}", actual_field.name);
            assert_eq!(actual_field.data_type, expected_field.data_type, "Field data_type mismatch for field {}", actual_field.name);
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
