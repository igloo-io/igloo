use crate::plan::LogicalPlan;
use sqlparser::ast::{Query, SetExpr, Statement, TableFactor};
use thiserror::Error;

#[derive(Error, Debug, PartialEq)]
pub enum PlanningError {
    #[error("Unsupported SQL statement: {0}")]
    UnsupportedStatement(String),
    #[error("Unsupported query structure: {0}")]
    UnsupportedQuery(String),
    #[error("No table found in FROM clause")]
    NoTableInFromClause,
    #[error("Unsupported table factor: {0}")]
    UnsupportedTableFactor(String),
    #[error("Expected exactly one table in FROM clause, found {0}")]
    MultipleTablesInFrom(usize),
    #[error("Unsupported expression: {0}")]
    UnsupportedExpression(String),
    #[error("Unsupported operator: {0}")]
    UnsupportedOperator(String),
    #[error("Unsupported literal value: {0}")]
    UnsupportedLiteral(String),
}

#[derive(Default)]
pub struct Planner {}

impl Planner {
    pub fn new() -> Self {
        Planner {}
    }

    pub fn plan_query(&self, statement: &Statement) -> Result<LogicalPlan, PlanningError> {
        match statement {
            Statement::Query(query) => self.plan_sql_query(query),
            _ => Err(PlanningError::UnsupportedStatement(
                format!("{:?}", statement)
            )),
        }
    }

    fn plan_sql_query(&self, query: &Query) -> Result<LogicalPlan, PlanningError> {
        match &*query.body {
            SetExpr::Select(select_statement) => {
                // Ensure there is exactly one table in the FROM clause
                if select_statement.from.len() != 1 {
                    return Err(PlanningError::MultipleTablesInFrom(
                        select_statement.from.len(),
                    ));
                }

                let table_with_joins = &select_statement.from[0];
                // For now, we don't support JOINs
                if !table_with_joins.joins.is_empty() {
                    return Err(PlanningError::UnsupportedQuery(
                        "JOINs are not yet supported".to_string(),
                    ));
                }

                match &table_with_joins.relation {
                    TableFactor::Table { name, .. } => {
                        // For simplicity, taking the first part of a potentially compound identifier
                        let table_name = name.0.get(0).map(|ident| ident.value.clone()).ok_or(
                            PlanningError::UnsupportedQuery("Table name is empty or invalid".to_string())
                        )?;
                        let mut plan = LogicalPlan::TableScan { table_name };

                        if let Some(selection) = &select_statement.selection {
                            let predicate_expr = self.plan_expression(selection)?;
                            plan = LogicalPlan::Filter {
                                input: std::sync::Arc::new(plan),
                                predicate: predicate_expr,
                            };
                        }
                        Ok(plan)
                    }
                    _ => Err(PlanningError::UnsupportedTableFactor(format!(
                        "{:?}",
                        table_with_joins.relation
                    ))),
                }
            }
            _ => Err(PlanningError::UnsupportedQuery(format!("{:?}", query.body))),
        }
    }

    fn plan_expression(&self, expr: &sqlparser::ast::Expr) -> Result<crate::plan::Expression, PlanningError> {
        use sqlparser::ast::{BinaryOperator, Expr as SqlExpr, Value as SqlValue};
        use crate::plan::{Expression, Operator};
        use std::sync::Arc;

        match expr {
            SqlExpr::Identifier(ident) => Ok(Expression::Column(ident.value.clone())),
            SqlExpr::Value(sql_value) => match sql_value {
                SqlValue::Number(s, _) => Ok(Expression::Literal(s.clone())),
                SqlValue::SingleQuotedString(s) => Ok(Expression::Literal(s.clone())),
                // SqlValue::Boolean(b) => Ok(Expression::Literal(b.to_string())), // If your Literal can store booleans
                _ => Err(PlanningError::UnsupportedLiteral(format!("{:?}", sql_value))),
            },
            SqlExpr::BinaryOp { left, op, right } => {
                let left_expr = Arc::new(self.plan_expression(left)?);
                let right_expr = Arc::new(self.plan_expression(right)?);
                let operator = match op {
                    BinaryOperator::Eq => Operator::Eq,
                    BinaryOperator::NotEq => Operator::NotEq,
                    BinaryOperator::Lt => Operator::Lt,
                    BinaryOperator::LtEq => Operator::LtEq,
                    BinaryOperator::Gt => Operator::Gt,
                    BinaryOperator::GtEq => Operator::GtEq,
                    BinaryOperator::And => Operator::And,
                    BinaryOperator::Or => Operator::Or,
                    _ => return Err(PlanningError::UnsupportedOperator(format!("{:?}", op))),
                };
                Ok(Expression::BinaryExpr {
                    left: left_expr,
                    op: operator,
                    right: right_expr,
                })
            }
            _ => Err(PlanningError::UnsupportedExpression(format!("{:?}", expr))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse_sql; // Assuming parse_sql is in the crate root (lib.rs)
    use crate::plan::{Expression, LogicalPlan, Operator}; // For assertions


    #[test]
    fn test_plan_simple_select() {
        // This test implicitly tests planning a query with no WHERE clause
        let sql = "SELECT a, b FROM my_table";
        let statements = parse_sql(sql).expect("Failed to parse SQL");
        assert_eq!(statements.len(), 1);

        let planner = Planner::new();
        let plan_result = planner.plan_query(&statements[0]);

        match plan_result {
            Ok(LogicalPlan::TableScan { table_name }) => {
                assert_eq!(table_name, "my_table");
            }
            Ok(other_plan) => {
                panic!("Expected TableScan, got {:?}", other_plan);
            }
            Err(e) => {
                panic!("Planning failed: {:?}", e);
            }
        }
    }

    #[test]
    fn test_unsupported_statement_insert() {
        let sql = "INSERT INTO my_table VALUES (1, 2)";
        let statements = parse_sql(sql).expect("Failed to parse SQL");
        let planner = Planner::new();
        let plan_result = planner.plan_query(&statements[0]);
        assert!(matches!(plan_result, Err(PlanningError::UnsupportedStatement(_))));
    }

    #[test]
    fn test_unsupported_query_union() {
        let sql = "SELECT * FROM t1 UNION SELECT * FROM t2";
         let statements = parse_sql(sql).expect("Failed to parse SQL");
        let planner = Planner::new();
        let plan_result = planner.plan_query(&statements[0]);
        assert!(matches!(plan_result, Err(PlanningError::UnsupportedQuery(_))),
                "Expected UnsupportedQuery error for UNION operations");
    }

    #[test]
    fn test_no_table_in_from() {
        // This case is tricky as sqlparser might parse "SELECT 1" differently
        // Let's try a select with a join but no initial table which might be an error earlier
        // or a select from a values list which is not a simple table.
        let sql = "SELECT * FROM (VALUES (1, 2)) AS t";
        let statements = parse_sql(sql).expect("Failed to parse SQL");
        let planner = Planner::new();
        let plan_result = planner.plan_query(&statements[0]);
        assert!(matches!(plan_result, Err(PlanningError::UnsupportedTableFactor(_))));
    }

    #[test]
    fn test_multiple_tables_no_join() {
        // sqlparser parses "SELECT * FROM t1, t2" as implicit cross join
        // current planner expects single table or explicit join syntax
        let sql = "SELECT * FROM t1, t2";
        let statements = parse_sql(sql).expect("Failed to parse SQL");
        let planner = Planner::new();
        let plan_result = planner.plan_query(&statements[0]);
        // if !matches!(&plan_result, Err(PlanningError::MultipleTablesInFrom(2))) {
        //     eprintln!("Unexpected plan_result: {:?}", plan_result);
        // }
        assert!(matches!(plan_result, Err(PlanningError::MultipleTablesInFrom(2))));
    }

    #[test]
    fn test_explicit_join() {
        let sql = "SELECT * FROM t1 JOIN t2 ON t1.id = t2.id";
        let statements = parse_sql(sql).expect("Failed to parse SQL");
        let planner = Planner::new();
        let plan_result = planner.plan_query(&statements[0]);
        assert!(matches!(plan_result, Err(PlanningError::UnsupportedQuery(s)) if s.contains("JOINs are not yet supported")));
    }

    #[test]
    fn test_plan_select_with_where_clause() {
        let sql = "SELECT a FROM my_table WHERE b > 10";
        let statements = parse_sql(sql).expect("Failed to parse SQL for WHERE clause test");
        assert_eq!(statements.len(), 1, "Should parse one statement");

        let planner = Planner::new();
        let plan_result = planner.plan_query(&statements[0]);

        match plan_result {
            Ok(LogicalPlan::Filter { input, predicate }) => {
                // Check the input of the Filter
                assert!(matches!(*input, LogicalPlan::TableScan { ref table_name, .. } if table_name == "my_table"), "Input to filter should be TableScan for 'my_table'");

                // Check the predicate
                match predicate {
                    Expression::BinaryExpr { left, op, right } => {
                        assert_eq!(op, Operator::Gt, "Expected '>' operator");
                        // Ensure we are comparing the correct Expression variants
                        assert_eq!(*left, Expression::Column("b".to_string()), "Expected column 'b'");
                        assert_eq!(*right, Expression::Literal("10".to_string()), "Expected literal '10'");
                    }
                    _ => panic!("Unexpected predicate format: {:?}", predicate),
                }
            }
            Err(e) => panic!("Failed to plan query: {:?}", e),
            Ok(other_plan) => panic!("Unexpected plan format: {:?}", other_plan),
        }
    }

    #[test]
    fn test_plan_where_with_and_operator() {
        let sql = "SELECT name FROM users WHERE id = 1 AND active = 'true'";
        let statements = parse_sql(sql).expect("SQL parsing failed");
        let planner = Planner::new();
        let plan = planner.plan_query(&statements[0]).expect("Planning failed");

        match plan {
            LogicalPlan::Filter { input, predicate } => {
                assert!(matches!(*input, LogicalPlan::TableScan { ref table_name, .. } if table_name == "users"), "Input should be TableScan for 'users'");
                match predicate {
                    Expression::BinaryExpr { left, op, right } => {
                        assert_eq!(op, Operator::And, "Expected AND operator");
                        // Check left side of AND: id = 1
                        match &*left {
                            Expression::BinaryExpr {left: l_left, op: l_op, right: l_right} => {
                                assert_eq!(*l_op, Operator::Eq, "Expected = operator for id = 1");
                                assert_eq!(**l_left, Expression::Column("id".to_string()), "Expected column 'id'");
                                assert_eq!(**l_right, Expression::Literal("1".to_string()), "Expected literal '1'");
                            }
                            _ => panic!("Left side of AND was not a BinaryExpr: {:?}", left),
                        }
                        // Check right side of AND: active = 'true'
                        match &*right {
                            Expression::BinaryExpr {left: r_left, op: r_op, right: r_right} => {
                                assert_eq!(*r_op, Operator::Eq, "Expected = operator for active = 'true'");
                                assert_eq!(**r_left, Expression::Column("active".to_string()), "Expected column 'active'");
                                assert_eq!(**r_right, Expression::Literal("true".to_string()), "Expected literal 'true'");
                            }
                            _ => panic!("Right side of AND was not a BinaryExpr: {:?}", right),
                        }
                    }
                    _ => panic!("Expected top-level predicate to be AND BinaryExpr, got {:?}", predicate),
                }
            }
            _ => panic!("Expected a Filter plan, got {:?}", plan),
        }
    }

    #[test]
    fn test_plan_where_with_string_literal() {
        let sql = "SELECT name FROM users WHERE status = 'active'";
        let statements = parse_sql(sql).expect("SQL parsing failed");
        let planner = Planner::new();
        let plan = planner.plan_query(&statements[0]).expect("Planning failed");

        match plan {
            LogicalPlan::Filter { input, predicate } => {
                assert!(matches!(*input, LogicalPlan::TableScan { ref table_name, .. } if table_name == "users"), "Input should be TableScan for 'users'");
                match predicate {
                    Expression::BinaryExpr { left, op, right } => {
                        assert_eq!(op, Operator::Eq, "Expected = operator");
                        assert_eq!(*left, Expression::Column("status".to_string()), "Expected column 'status'");
                        assert_eq!(*right, Expression::Literal("active".to_string()), "Expected literal 'active'");
                    }
                    _ => panic!("Expected predicate to be BinaryExpr, got {:?}", predicate),
                }
            }
            _ => panic!("Expected a Filter plan, got {:?}", plan),
        }
    }

    #[test]
    fn test_unsupported_expression_in_where() {
        // Example: Using a subquery in WHERE, which we don't support yet
        let sql = "SELECT name FROM users WHERE id = (SELECT MAX(id) FROM other_users)";
        let statements = parse_sql(sql).expect("SQL parsing failed");
        let planner = Planner::new();
        let plan_result = planner.plan_query(&statements[0]);
        assert!(matches!(plan_result, Err(PlanningError::UnsupportedExpression(_))));
    }
}
