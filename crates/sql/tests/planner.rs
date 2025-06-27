use igloo_common::catalog::Schema;
use igloo_sql::{LogicalExpr, LogicalPlan, Planner};
use std::sync::Arc;

#[test]
fn test_simple_select() {
    let planner = Planner::new();
    let sql = "SELECT col1, col2 FROM my_table;";
    let result = planner.sql_to_logical_plan(sql);

    assert!(result.is_ok());
    let plan = result.unwrap();

    let expected_schema = Arc::new(Schema::empty()); // Dummy schema

    if let LogicalPlan::Projection { expr, input } = plan {
        assert_eq!(expr.len(), 2);
        assert_eq!(expr[0], LogicalExpr::Column { name: "col1".to_string() });
        assert_eq!(expr[1], LogicalExpr::Column { name: "col2".to_string() });

        if let LogicalPlan::TableScan { table_name, projected_schema } = *input {
            assert_eq!(table_name, "my_table");
            assert_eq!(projected_schema, expected_schema);
        } else {
            panic!("Input to Projection should be a TableScan");
        }
    } else {
        panic!("Expected a Projection plan");
    }
}

#[test]
fn test_select_with_alias() {
    let planner = Planner::new();
    let sql = "SELECT a AS b FROM my_table;";
    let result = planner.sql_to_logical_plan(sql);

    assert!(result.is_ok());
    let plan = result.unwrap();

    if let LogicalPlan::Projection { expr, .. } = plan {
        assert_eq!(expr.len(), 1);
        assert_eq!(expr[0], LogicalExpr::Column { name: "b".to_string() });
    } else {
        panic!("Expected a Projection plan");
    }
}


#[test]
fn test_invalid_sql() {
    let planner = Planner::new();
    let sql = "SELECT FROM my_table;"; // Invalid SQL syntax
    let result = planner.sql_to_logical_plan(sql);
    assert!(result.is_err());
    if let Err(e) = result {
        // sqlparser-rs parses "SELECT FROM my_table" as SELECT "FROM" AS "my_table" (no FROM clause)
        // Our planner then correctly identifies that the FROM clause is missing.
        assert_eq!(e.to_string(), "Planning error: FROM clause is required");
    }
}

#[test]
fn test_unsupported_statement_insert() {
    let planner = Planner::new();
    let sql = "INSERT INTO my_table (col1) VALUES (1);";
    let result = planner.sql_to_logical_plan(sql);
    assert!(result.is_err());
    if let Err(e) = result {
        assert_eq!(e.to_string(), "Planning error: Unsupported SQL statement type");
    }
}

#[test]
fn test_unsupported_statement_create() {
    let planner = Planner::new();
    let sql = "CREATE TABLE my_table (col1 INT);";
    let result = planner.sql_to_logical_plan(sql);
    assert!(result.is_err());
    if let Err(e) = result {
        assert_eq!(e.to_string(), "Planning error: Unsupported SQL statement type");
    }
}

#[test]
fn test_multiple_statements() {
    let planner = Planner::new();
    let sql = "SELECT col1 FROM table1; SELECT col2 FROM table2;";
    let result = planner.sql_to_logical_plan(sql);
    assert!(result.is_err());
    if let Err(e) = result {
        assert_eq!(e.to_string(), "Planning error: Exactly one statement is required");
    }
}

#[test]
fn test_no_from_clause() {
    let planner = Planner::new();
    let sql = "SELECT col1;";
    let result = planner.sql_to_logical_plan(sql);
    assert!(result.is_err());
    if let Err(e) = result {
        assert_eq!(e.to_string(), "Planning error: FROM clause is required");
    }
}

#[test]
fn test_unsupported_select_item() {
    let planner = Planner::new();
    let sql = "SELECT * FROM my_table;"; // Wildcard not yet supported
    let result = planner.sql_to_logical_plan(sql);
    assert!(result.is_err());
    if let Err(e) = result {
        assert_eq!(e.to_string(), "Planning error: Unsupported SELECT item");
    }
}

#[test]
fn test_unsupported_table_factor() {
    let planner = Planner::new();
    let sql = "SELECT col1 FROM (SELECT col2 FROM table2);"; // Subquery in FROM
    let result = planner.sql_to_logical_plan(sql);
    assert!(result.is_err());
    if let Err(e) = result {
        assert_eq!(e.to_string(), "Planning error: Unsupported table factor in FROM clause");
    }
}
