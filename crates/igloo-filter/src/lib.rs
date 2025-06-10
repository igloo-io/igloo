use std::sync::Arc;
use datafusion_expr::Expr;
use igloo_core::operator::Operator;

pub struct FilterOperator {
    pub input: Arc<dyn Operator + Send + Sync>,
    pub predicate: Expr,
}
