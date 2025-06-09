//! Engine crate
//!
//! Implements the core query engine for Igloo.
//!
//! # Example
//! ```rust
//! // Example usage will go here once implemented
//! ```
//!
//! # TODO
//! Implement query engine logic

use igloo_connector_filesystem::{CsvTable, TableProvider}; // Added TableProvider
pub mod logical_plan;
pub use logical_plan::{create_logical_plan, LogicalPlan};

pub trait ExecutionPlan {
    // Adjusted return type to include lifetime tied to `self`
    fn execute(&self) -> Result<Box<dyn Iterator<Item = Vec<String>> + '_>, String>;
}

pub struct ScanExec {
    pub path: String,
}

impl ExecutionPlan for ScanExec {
    fn execute(&self) -> Result<Box<dyn Iterator<Item = Vec<String>> + '_>, String> {
        let table = CsvTable::new(&self.path); // Changed to &self.path
                                               // table.scan() now correctly calls TableProvider::scan if TableProvider is in scope
                                               // And CsvTable::scan returns Result<Box<dyn Iterator<Item = Row>>> which is Vec<String>
                                               // Error type also matches if CsvTable::scan returns our local Result which is igloo_common::error::Error
                                               // However, ScanExec::execute returns Result<..., String> for error.
                                               // CsvTable::scan (from origin/main) returns Result<..., igloo_common::error::Error>
                                               // Need to map the error type.
        table.scan().map_err(|e| e.to_string())
    }
}

pub struct FilterExec {
    pub input: Box<dyn ExecutionPlan>,
    pub predicate: Box<dyn Fn(Vec<String>) -> bool>,
}

impl ExecutionPlan for FilterExec {
    fn execute(&self) -> Result<Box<dyn Iterator<Item = Vec<String>> + '_>, String> {
        let input_data = self.input.execute()?;

        let predicate_ref = &self.predicate;
        let filtered_iter =
            input_data.filter(move |row: &Vec<String>| (*predicate_ref)(row.clone()));
        Ok(Box::new(filtered_iter))
    }
}

pub struct ProjectionExec {
    pub input: Box<dyn ExecutionPlan>,
    pub columns: Vec<usize>,
}

impl ExecutionPlan for ProjectionExec {
    fn execute(&self) -> Result<Box<dyn Iterator<Item = Vec<String>> + '_>, String> {
        let input_iter = self.input.execute()?;

        let columns_clone = self.columns.clone();

        let projected_iter = input_iter.map(move |row: Vec<String>| {
            let mut projected_row = Vec::with_capacity(columns_clone.len());
            for &index in &columns_clone {
                projected_row.push(row.get(index).cloned().unwrap_or_default());
            }
            projected_row
        });

        Ok(Box::new(projected_iter))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn sample_test() {
        assert_eq!(2 + 2, 4);
    }

    #[test]
    fn test_end_to_end_query() -> Result<(), String> {
        let mut temp_file =
            NamedTempFile::new().map_err(|e| format!("Failed to create temp file: {}", e))?;
        writeln!(temp_file, "id,name,score")
            .map_err(|e| format!("Failed to write to temp file: {}", e))?;
        writeln!(temp_file, "1,a,100")
            .map_err(|e| format!("Failed to write to temp file: {}", e))?;
        writeln!(temp_file, "2,b,80")
            .map_err(|e| format!("Failed to write to temp file: {}", e))?;
        writeln!(temp_file, "3,c,95")
            .map_err(|e| format!("Failed to write to temp file: {}", e))?;
        temp_file.flush().map_err(|e| format!("Failed to flush temp file: {}", e))?;
        let file_path =
            temp_file.path().to_str().ok_or("Temp file path is not valid UTF-8")?.to_string();

        let scan_exec = ScanExec { path: file_path };

        let filter_exec = FilterExec {
            input: Box::new(scan_exec),
            predicate: Box::new(|row: Vec<String>| {
                if row.first() == Some(&"id".to_string()) {
                    return false;
                }
                if let Some(score_str) = row.get(2) {
                    if let Ok(score) = score_str.parse::<i32>() {
                        return score > 90;
                    }
                }
                false
            }),
        };

        let projection_exec = ProjectionExec { input: Box::new(filter_exec), columns: vec![1] };

        let mut results_iter = projection_exec.execute()?;

        assert_eq!(results_iter.next(), Some(vec!["a".to_string()]));
        assert_eq!(results_iter.next(), Some(vec!["c".to_string()]));
        assert_eq!(results_iter.next(), None);

        Ok(())
    }

    #[test]
    fn test_create_logical_plan() {
        let sql = "SELECT a FROM my_table WHERE b > 10";

        let dialect = GenericDialect {};
        let ast_statements = Parser::parse_sql(&dialect, sql).unwrap();

        assert_eq!(ast_statements.len(), 1, "Expected one SQL statement");
        let ast = ast_statements.into_iter().next().unwrap();

        let logical_plan = create_logical_plan(ast).unwrap();

        match logical_plan {
            LogicalPlan::Projection { expr, input } => {
                assert_eq!(expr, vec!["a".to_string()]);
                match *input {
                    LogicalPlan::Filter { predicate, input } => {
                        assert_eq!(predicate, "b > 10".to_string());
                        match *input {
                            LogicalPlan::TableScan { table_name } => {
                                assert_eq!(table_name, "my_table".to_string());
                            }
                            _ => panic!("Expected TableScan, got {:?}", *input),
                        }
                    }
                    _ => panic!("Expected Filter, got {:?}", *input),
                }
            }
            _ => panic!("Expected Projection, got {:?}", logical_plan),
        }
    }
}
