use crate::logical_plan::{LogicalExpr, LogicalPlan};
use igloo_common::{catalog::Schema, error::Error};
use sqlparser::{
    ast::{Ident, Query, SelectItem, SetExpr, Statement, TableFactor},
    dialect::GenericDialect,
    parser::Parser,
};
use std::sync::Arc;

#[derive(Default)]
pub struct Planner {}

impl Planner {
    pub fn new() -> Self {
        Self {}
    }

    pub fn sql_to_logical_plan(&self, sql: &str) -> Result<LogicalPlan, Error> {
        let dialect = GenericDialect {};
        let statements = Parser::parse_sql(&dialect, sql)
            .map_err(|e| Error::Parse(e.to_string()))?;

        if statements.len() != 1 {
            return Err(Error::Plan(
                "Exactly one statement is required".to_string(),
            ));
        }

        self.statement_to_logical_plan(statements[0].clone())
    }

    fn statement_to_logical_plan(&self, statement: Statement) -> Result<LogicalPlan, Error> {
        match statement {
            Statement::Query(query) => self.query_to_logical_plan(*query),
            _ => Err(Error::Plan(
                "Unsupported SQL statement type".to_string(),
            )),
        }
    }

    fn query_to_logical_plan(&self, query: Query) -> Result<LogicalPlan, Error> {
        let Query { body, .. } = query;
        match *body {
            SetExpr::Select(select) => {
                let table_scan = if let Some(from_table) = select.from.get(0) {
                    match &from_table.relation {
                        TableFactor::Table { name, .. } => {
                            let table_name = name.0.get(0).map_or_else(
                                || Err(Error::Plan("Table name not found".to_string())),
                                |ident| Ok(ident.value.clone()),
                            )?;
                            // Dummy schema for now
                            let dummy_schema = Arc::new(Schema::empty());
                            LogicalPlan::TableScan {
                                table_name,
                                projected_schema: dummy_schema,
                            }
                        }
                        _ => {
                            return Err(Error::Plan(
                                "Unsupported table factor in FROM clause".to_string(),
                            ))
                        }
                    }
                } else {
                    return Err(Error::Plan("FROM clause is required".to_string()));
                };

                let projection_exprs: Vec<LogicalExpr> = select
                    .projection
                    .into_iter()
                    .map(|item| match item {
                        SelectItem::UnnamedExpr(expr) => match expr {
                            sqlparser::ast::Expr::Identifier(Ident { value, .. }) => {
                                Ok(LogicalExpr::Column { name: value })
                            }
                            _ => Err(Error::Plan(
                                "Unsupported expression in SELECT".to_string(),
                            )),
                        },
                        SelectItem::ExprWithAlias { expr, alias } => match expr {
                            sqlparser::ast::Expr::Identifier(Ident { .. }) => { // value removed
                                Ok(LogicalExpr::Column { name: alias.value }) // Use alias if available
                            }
                            _ => Err(Error::Plan(
                                "Unsupported aliased expression in SELECT".to_string(),
                            )),
                        },
                        _ => Err(Error::Plan("Unsupported SELECT item".to_string())),
                    })
                    .collect::<Result<Vec<LogicalExpr>, Error>>()?;

                Ok(LogicalPlan::Projection {
                    expr: projection_exprs,
                    input: Box::new(table_scan),
                })
            }
            _ => Err(Error::Plan("Unsupported query type".to_string())),
        }
    }
}
