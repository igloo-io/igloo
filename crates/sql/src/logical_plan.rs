use igloo_common::catalog::SchemaRef;

#[derive(Debug, PartialEq, Clone)]
pub enum LogicalPlan {
    Projection {
        expr: Vec<LogicalExpr>,
        input: Box<LogicalPlan>,
    },
    TableScan {
        table_name: String,
        projected_schema: SchemaRef,
    },
}

#[derive(Debug, PartialEq, Clone)]
pub enum LogicalExpr {
    Column { name: String },
}
