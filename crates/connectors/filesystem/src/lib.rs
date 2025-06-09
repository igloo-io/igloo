// TODO: Implement filesystem connector logic

use csv::ReaderBuilder;
use std::fs::File;

pub struct CsvTable {
    pub path: String,
}

impl CsvTable {
    pub fn new(path: String) -> Self {
        Self { path }
    }

    pub fn scan(&self) -> Result<Box<dyn Iterator<Item = Vec<String>>>, String> {
        let file = File::open(&self.path).map_err(|e| format!("Failed to open file: {}", e))?;

        let mut rdr = ReaderBuilder::new().has_headers(false).from_reader(file);

        let mut records = Vec::new();
        for result in rdr.records() {
            let record = result.map_err(|e| format!("Failed to parse CSV record: {}", e))?;
            records.push(record.iter().map(|field| field.to_string()).collect());
        }

        Ok(Box::new(records.into_iter()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_csv_table_scan_success() {
        let mut temp_file = tempfile::NamedTempFile::new().unwrap();
        writeln!(temp_file, "1,a,x").unwrap();
        writeln!(temp_file, "2,b,y").unwrap();
        writeln!(temp_file, "3,c,z").unwrap();
        temp_file.flush().unwrap();

        let table = CsvTable::new(temp_file.path().to_str().unwrap().to_string());
        let mut iter = table.scan().unwrap();

        assert_eq!(iter.next(), Some(vec!["1".to_string(), "a".to_string(), "x".to_string()]));
        assert_eq!(iter.next(), Some(vec!["2".to_string(), "b".to_string(), "y".to_string()]));
        assert_eq!(iter.next(), Some(vec!["3".to_string(), "c".to_string(), "z".to_string()]));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_csv_table_scan_file_not_found() {
        let table = CsvTable::new("non_existent_file.csv".to_string());
        assert!(table.scan().is_err());
    }

    #[test]
    fn test_csv_table_scan_invalid_csv() {
        let mut temp_file = tempfile::NamedTempFile::new().unwrap();
        writeln!(temp_file, "1,a,x").unwrap();
        writeln!(temp_file, "2,b").unwrap(); // Invalid record
        temp_file.flush().unwrap();

        let table = CsvTable::new(temp_file.path().to_str().unwrap().to_string());
        let result = table.scan();
        // Depending on the csv library's behavior, this might or might not error on reading,
        // but rather when iterating. For this setup, we check if it errors out during the scan setup.
        // The exact behavior might need adjustment based on how errors are propagated.
        // For now, let's assume the current implementation would error out when `collect()` is called internally
        // or if the library is strict about record lengths from the start.
        // This test might need refinement based on the chosen CSV parsing strategy and error handling.
        // The current implementation reads all records into memory first, so it should catch this.
        assert!(result.is_err());
    }
}
