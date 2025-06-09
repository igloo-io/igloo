use csv::ReaderBuilder;
use igloo_common::error::Error; // Assuming Error is pub in igloo_common::error
use std::fs::File;

// Define a local Result alias
pub type Result<T> = std::result::Result<T, Error>;

// For now, a Row is just a vector of strings.
pub type Row = Vec<String>;

// A trait for any component that can provide data.
pub trait TableProvider {
    /// Scan the table and return an iterator over the rows.
    fn scan(&self) -> Result<Box<dyn Iterator<Item = Row>>>;
}

/// A TableProvider that reads from a CSV file.
pub struct CsvTable {
    path: String,
    has_header: bool,
}

impl CsvTable {
    pub fn new(path: &str) -> Self {
        Self { path: path.to_string(), has_header: true } // Default to has_header = true as per origin/main
    }

    pub fn new_with_header(path: String, has_header: bool) -> Self {
        // path can be String
        Self { path, has_header }
    }
}

impl TableProvider for CsvTable {
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
    use std::io::Write;
    use tempfile::NamedTempFile; // For creating temporary files easily

    #[test]
    fn test_scan_with_header_uses_tempfile() {
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(temp_file, "id,name").unwrap();
        writeln!(temp_file, "1,foo").unwrap();
        writeln!(temp_file, "2,bar").unwrap();
        temp_file.flush().unwrap();

        let table = CsvTable::new_with_header(temp_file.path().to_str().unwrap().to_string(), true);
        let mut iterator = table.scan().unwrap();

        assert_eq!(iterator.next(), Some(vec!["1".to_string(), "foo".to_string()]));
        assert_eq!(iterator.next(), Some(vec!["2".to_string(), "bar".to_string()]));
        assert!(iterator.next().is_none());
    }

    #[test]
    fn test_scan_no_header_uses_tempfile() {
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(temp_file, "a,b").unwrap();
        writeln!(temp_file, "c,d").unwrap();
        temp_file.flush().unwrap();

        let table =
            CsvTable::new_with_header(temp_file.path().to_str().unwrap().to_string(), false);
        let mut iterator = table.scan().unwrap();

        assert_eq!(iterator.next(), Some(vec!["a".to_string(), "b".to_string()]));
        assert_eq!(iterator.next(), Some(vec!["c".to_string(), "d".to_string()]));
        assert!(iterator.next().is_none());
    }

    #[test]
    fn test_scan_csv_file_not_found() {
        // Use a non-existent file path
        let table = CsvTable::new("non_existent_file.csv");
        let result = table.scan();
        assert!(result.is_err());
        if let Err(e) = result {
            // Check if the error message indicates file not found.
            // This check is more robust than exact string matching.
            let err_msg = e.to_string().to_lowercase();
            assert!(
                err_msg.contains("no such file or directory")
                    || err_msg.contains("cannot find the file")
            );
        }
    }

    #[test]
    fn test_csv_table_scan_invalid_csv() {
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(temp_file, "1,a,x").unwrap();
        writeln!(temp_file, "2,b").unwrap(); // Invalid record (different number of fields)
        temp_file.flush().unwrap();

        // Scan without headers, so the CSV parser might complain about inconsistent column numbers.
        let table =
            CsvTable::new_with_header(temp_file.path().to_str().unwrap().to_string(), false);
        let result = table.scan();
        // The csv crate's default behavior is to error out if records have inconsistent lengths.
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(e.to_string().contains("CSV error")); // Generic check for CSV related error
        }
    }
}
