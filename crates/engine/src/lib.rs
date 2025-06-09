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

pub mod parser;

#[cfg(test)]
mod tests {
    use super::parser::parse_sql;

    #[test]
    fn sample_test() {
        assert_eq!(2 + 2, 4);
    }

    #[test]
    fn test_simple_select() {
        let sql = "SELECT a, b FROM table_1 WHERE a > 10";
        let ast = parse_sql(sql);
        assert!(ast.is_ok());
    }
}
