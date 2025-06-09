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

use igloo_connector_filesystem::CsvTable; // Corrected import

pub trait ExecutionPlan {
    // Adjusted return type to include lifetime tied to `self`
    fn execute(&self) -> Result<Box<dyn Iterator<Item = Vec<String>> + '_>, String>;
}

pub struct ScanExec {
    pub path: String,
}

impl ExecutionPlan for ScanExec {
    fn execute(&self) -> Result<Box<dyn Iterator<Item = Vec<String>> + '_>, String> {
        let table = CsvTable::new(self.path.clone());
        table.scan()
    }
}

pub struct FilterExec {
    pub input: Box<dyn ExecutionPlan>,
    pub predicate: Box<dyn Fn(Vec<String>) -> bool>,
}

impl ExecutionPlan for FilterExec {
    fn execute(&self) -> Result<Box<dyn Iterator<Item = Vec<String>> + '_>, String> {
        let input_data = self.input.execute()?; // Renamed to avoid unused_variable warning if only used in closure

        // predicate_ref is captured by the closure. The closure itself is 'move',
        // but predicate_ref is a reference with the lifetime of `self`.
        // By specifying `+ '_` in the return type, we allow this.
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

        // To handle lifetimes correctly for the returned Box<dyn Iterator>,
        // especially if it needs to be 'static, we should ensure that data captured by
        // the closure is also 'static. `self.columns` is `Vec<usize>`.
        // If we capture `&self.columns`, the closure is tied to the lifetime of `self`.
        // To make it 'static, we should clone `self.columns` and move the clone
        // into the closure.
        let columns_clone = self.columns.clone();

        let projected_iter = input_iter.map(move |row: Vec<String>| {
            let mut projected_row = Vec::with_capacity(columns_clone.len());
            for &index in &columns_clone {
                // .get(index) returns Option<&String>, then .cloned() to Option<String>
                // .unwrap_or_default() handles out-of-bounds by providing a default String (empty).
                // This is a simple way to handle potential out-of-bounds access,
                // though a planner should ideally ensure indices are valid.
                // Cloning is necessary as we are creating a new Vec<String> from owned values.
                projected_row.push(row.get(index).cloned().unwrap_or_default());
            }
            projected_row
        });

        Ok(Box::new(projected_iter))
    }
}

#[cfg(test)]
mod tests {
    use super::*; // Imports ExecutionPlan, ScanExec, FilterExec, ProjectionExec
                  // use std::fs::File; // Removed unused import
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn sample_test() {
        assert_eq!(2 + 2, 4);
    }

    #[test]
    fn test_end_to_end_query() -> Result<(), String> {
        // 1. Create a temporary CSV file
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

        // 2. Manually Construct Physical Plan
        let scan_exec = ScanExec { path: file_path };

        let filter_exec = FilterExec {
            input: Box::new(scan_exec),
            predicate: Box::new(|row: Vec<String>| {
                // Use .first() instead of .get(0) as per clippy recommendation
                if row.first() == Some(&"id".to_string()) {
                    // Skip header
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

        // ProjectionExec: project name (column 1)
        let projection_exec = ProjectionExec {
            input: Box::new(filter_exec),
            columns: vec![1], // Index of the 'name' column
        };

        // 3. Execute the Query
        let mut results_iter = projection_exec.execute()?;

        // 4. Verify Results
        assert_eq!(results_iter.next(), Some(vec!["a".to_string()]));
        assert_eq!(results_iter.next(), Some(vec!["c".to_string()]));
        assert_eq!(results_iter.next(), None);

        // temp_file is automatically removed when it goes out of scope.
        Ok(())
    }
}
