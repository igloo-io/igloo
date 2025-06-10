use csv::ReaderBuilder;
use igloo_common::error::Error;
use std::fs::File;

// Define a local Result alias
pub type Result<T> = std::result::Result<T, Error>;

pub mod parquet;

// For now, a Row is just a vector of strings.
pub type Row = Vec<String>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataType {
    String,
    Int64,
    Float64,
    Boolean,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Field {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    pub fields: Vec<Field>,
}

// A trait for any component that can provide data.
pub trait TableProvider {
    fn schema(&self) -> Result<Schema>;
    fn scan(&self) -> Result<Box<dyn Iterator<Item = Row>>>;
}

/// A TableProvider that reads from a CSV file.
pub struct CsvTable {
    path: String,
    has_header: bool,
}

impl CsvTable {
    pub fn new(path: &str) -> Self {
        Self { path: path.to_string(), has_header: true }
    }

    pub fn new_with_header(path: &str, has_header: bool) -> Self {
        Self { path: path.to_string(), has_header }
    }
}

impl TableProvider for CsvTable {
    fn schema(&self) -> Result<Schema> {
        if !self.has_header {
            return Ok(Schema { fields: vec![] });
        }

        let file = File::open(&self.path).map_err(|e| Error::Unknown(format!("Failed to open file {}: {}", self.path, e)))?;
        let mut rdr = ReaderBuilder::new().has_headers(true).from_reader(file);

        match rdr.headers() {
            Ok(headers) => {
                let fields = headers.iter()
                    .map(|header_name| Field {
                        name: header_name.to_string(),
                        data_type: DataType::String,
                        nullable: true,
                    })
                    .collect();
                Ok(Schema { fields })
            }
            Err(e) => Err(Error::Unknown(format!("Failed to read headers from {}: {}", self.path, e))),
        }
    }

    fn scan(&self) -> Result<Box<dyn Iterator<Item = Row>>> {
        let file = File::open(&self.path).map_err(|e| Error::Unknown(e.to_string()))?;
        let mut rdr = ReaderBuilder::new().has_headers(self.has_header).from_reader(file);

        let mut rows: Vec<Row> = Vec::new();
        for result in rdr.records() {
            let record = result.map_err(|e| Error::Unknown(e.to_string()))?;
            let row: Row = record.iter().map(|field| field.to_string()).collect();
            rows.push(row);
        }
        Ok(Box::new(rows.into_iter()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scan_csv_with_header() {
        // Create the CsvTable instance for our test file, explicitly stating it has a header
        let table = CsvTable::new_with_header("test_data.csv", true);
        let mut iterator = table.scan().unwrap();
        let first_row = iterator.next().unwrap();
        assert_eq!(first_row, vec!["1".to_string(), "foo".to_string()]);
        let second_row = iterator.next().unwrap();
        assert_eq!(second_row, vec!["2".to_string(), "bar".to_string()]);
        assert!(iterator.next().is_none());
    }

    #[test]
    fn test_scan_csv_no_header() {
        let temp_dir = std::env::temp_dir();
        let file_path = temp_dir.join("test_no_header.csv");
        {
            let mut wtr = csv::Writer::from_path(&file_path).unwrap();
            wtr.write_record(["a", "b"]).unwrap();
            wtr.write_record(["c", "d"]).unwrap();
            wtr.flush().unwrap();
        }

        let table = CsvTable::new_with_header(file_path.to_str().unwrap(), false);
        let mut iterator = table.scan().unwrap();
        let first_row = iterator.next().unwrap();
        assert_eq!(first_row, vec!["a".to_string(), "b".to_string()]);
        let second_row = iterator.next().unwrap();
        assert_eq!(second_row, vec!["c".to_string(), "d".to_string()]);
        assert!(iterator.next().is_none());
        std::fs::remove_file(file_path).unwrap();
    }

    #[test]
    fn test_scan_csv_file_not_found() {
        let table = CsvTable::new("non_existent_file.csv");
        let result = table.scan();
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(
                e.to_string().contains("No such file or directory")
                    || e.to_string().contains("The system cannot find the file specified")
            );
        }
    }
}
