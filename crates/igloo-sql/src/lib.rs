pub mod plan;
pub mod planner;

use sqlparser::dialect::GenericDialect;
use sqlparser::parser::{Parser, ParserError};
use sqlparser::ast::Statement;

/// Parses a SQL string into a vector of statements.
///
/// # Arguments
///
/// * `sql` - A string slice that holds the SQL to parse.
///
/// # Returns
///
/// A `Result` containing either a vector of `sqlparser::ast::Statement` objects
/// if parsing is successful, or a `sqlparser::parser::ParserError` if it fails.
pub fn parse_sql(sql: &str) -> Result<Vec<Statement>, ParserError> {
    let dialect = GenericDialect {}; // Or specific dialect
    Parser::parse_sql(&dialect, sql)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_select() {
        let sql = "SELECT * FROM my_table";
        match parse_sql(sql) {
            Ok(statements) => {
                assert_eq!(statements.len(), 1, "Should produce one statement");
                // You could add more assertions here to check the statement type, table name, etc.
                if let Statement::Query(query) = &statements[0] {
                    assert_eq!(query.to_string(), "SELECT * FROM my_table");
                } else {
                    panic!("Expected a Query statement");
                }
            }
            Err(e) => {
                panic!("Failed to parse SQL: {:?}", e);
            }
        }
    }

    #[test]
    fn test_parse_invalid_sql() {
        let sql = "SELECT ((FROM table"; // Clearly invalid SQL
        let result = parse_sql(sql);
        // Optional: print if it unexpectedly succeeds, for debugging.
        // if result.is_ok() {
        //     eprintln!("Unexpected success for 'SELECT ((FROM table': {:?}", result.as_ref().unwrap());
        // }
        assert!(result.is_err(), "Should return an error for 'SELECT ((FROM table'");
    }

    #[test]
    fn test_parse_multiple_statements() {
        let sql = "SELECT * FROM table1; INSERT INTO table2 VALUES (1, 2);";
        match parse_sql(sql) {
            Ok(statements) => {
                assert_eq!(statements.len(), 2, "Should produce two statements");
            }
            Err(e) => {
                panic!("Failed to parse SQL: {:?}", e);
            }
        }
    }
}
