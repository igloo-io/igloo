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

pub mod logical_plan;
pub use logical_plan::{create_logical_plan, LogicalPlan};

pub mod physical_plan; // Added
pub use physical_plan::PhysicalPlan; // Added

#[cfg(test)]
mod tests {
    use super::*; // To bring create_logical_plan and LogicalPlan into scope
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    #[test]
    fn sample_test() {
        assert_eq!(2 + 2, 4);
    }

    #[test]
    fn test_create_logical_plan() {
        let sql = "SELECT a FROM my_table WHERE b > 10";

        // Parse the SQL string using sqlparser
        let dialect = GenericDialect {}; // Or any other dialect
        let ast_statements = Parser::parse_sql(&dialect, sql).unwrap();

        // We expect a single statement for this test SQL
        assert_eq!(ast_statements.len(), 1, "Expected one SQL statement");
        let ast = ast_statements.into_iter().next().unwrap();

        // This is the function you need to implement (already done)
        let logical_plan = create_logical_plan(ast).unwrap();

        // Verify the plan has the correct structure
        // Projection -> Filter -> TableScan
        match logical_plan {
            LogicalPlan::Projection { expr, input } => {
                // Check projection expressions (optional, but good for thoroughness)
                assert_eq!(expr, vec!["a".to_string()]);
                match *input {
                    LogicalPlan::Filter { predicate, input } => {
                        // Check predicate (optional)
                        assert_eq!(predicate, "b > 10".to_string()); // Based on current simple string representation
                        match *input {
                            LogicalPlan::TableScan { table_name } => {
                                // Check table name (optional)
                                assert_eq!(table_name, "my_table".to_string());
                                // The structure is correct if we reach here
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
