// In crates/engine/src/parser.rs
use igloo_common::error::Result;
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

pub fn parse_sql(sql: &str) -> Result<Statement> {
    let dialect = GenericDialect {};
    let mut ast = Parser::parse_sql(&dialect, sql)?;
    // The parser can return multiple statements; we only support one for now.
    Ok(ast.pop().unwrap())
}
