use std::sync::Arc;

/// Represents a logical query plan.
#[derive(Debug, PartialEq)]
pub enum LogicalPlan {
    /// Scans a table.
    TableScan {
        table_name: String,
        // Optional: projection (list of column indices or names to output)
        // projection: Option<Vec<usize>>,
    },
    /// Filters a plan based on an expression.
    Filter {
        input: Arc<LogicalPlan>,
        predicate: Expression,
    },
    // Other plan nodes like Projection, Join, Aggregate, Sort, etc. will be added later.
}

/// Represents an expression in a query plan.
#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    /// A literal value (e.g., string, number).
    Literal(String), // Simplified to String for now, can be an enum for different types
    /// A column reference by name.
    Column(String),
    /// A binary expression (e.g., a = b, a + b).
    BinaryExpr {
        left: Arc<Expression>,
        op: Operator,
        right: Arc<Expression>,
    },
    // Other expression types like UnaryOp, FunctionCall, Case, etc.
}

/// Represents a binary operator.
#[derive(Debug, Clone, PartialEq, Eq, Hash)] // Added Eq, Hash for potential use in HashMaps/Sets
pub enum Operator {
    Eq,    // Equal
    NotEq, // Not Equal
    Lt,    // Less Than
    LtEq,  // Less Than or Equal
    Gt,    // Greater Than
    GtEq,  // Greater Than or Equal
    And,   // Logical AND
    Or,    // Logical OR
    // Other operators like Plus, Minus, Multiply, Divide, Modulo, StringConcat etc.
}

// Example of how you might represent literal values more strongly:
// #[derive(Debug, Clone, PartialEq)]
// pub enum LiteralValue {
//     Null,
//     Boolean(bool),
//     Int64(i64),
//     Float64(f64),
//     String(String),
//     // Date, Timestamp, Interval, etc.
// }
//
// And then Expression::Literal(LiteralValue)
