//! Physical planner for converting logical plans to physical plans

use crate::operators::{ParquetScanExec, ProjectionExec, FilterExec, HashJoinExec};
use crate::physical_plan::{ExecutionPlan, PhysicalPlan};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::{LogicalPlan, Expr, JoinType as LogicalJoinType};
use datafusion::physical_expr::create_physical_expr;
use datafusion::execution::context::SessionContext;
use igloo_common::catalog::MemoryCatalog;
use std::sync::Arc;

/// Physical planner for converting logical plans to executable physical plans
pub struct PhysicalPlanner {
    catalog: Arc<MemoryCatalog>,
    session_ctx: Arc<SessionContext>,
}

impl PhysicalPlanner {
    pub fn new(catalog: Arc<MemoryCatalog>, session_ctx: Arc<SessionContext>) -> Self {
        Self { catalog, session_ctx }
    }

    pub async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        match logical_plan {
            LogicalPlan::TableScan(scan) => {
                let table_name = &scan.table_name;
                
                // Look up table in catalog
                let table_provider = self.catalog.get_table(table_name)
                    .ok_or_else(|| DataFusionError::Plan(format!("Table '{}' not found", table_name)))?;
                
                let schema = table_provider.schema();
                
                // For now, assume all tables are Parquet files
                // In a real implementation, we'd check the table provider type
                let file_path = format!("data/{}.parquet", table_name);
                
                let projection = scan.projection.clone();
                let projected_schema = if let Some(ref proj) = projection {
                    Arc::new(schema.project(proj)?)
                } else {
                    schema
                };
                
                let parquet_scan = ParquetScanExec::new(file_path, projected_schema, projection);
                Ok(Arc::new(PhysicalPlan::ParquetScan(parquet_scan)))
            }
            
            LogicalPlan::Projection(projection) => {
                let input = self.create_physical_plan(&projection.input).await?;
                let input_schema = input.schema();
                
                let mut physical_exprs = Vec::new();
                let mut output_fields = Vec::new();
                
                for expr in &projection.expr {
                    let physical_expr = create_physical_expr(
                        expr,
                        &input_schema,
                        &self.session_ctx.state().execution_props(),
                    )?;
                    
                    let name = expr.display_name()?;
                    let field = arrow::datatypes::Field::new(
                        &name,
                        physical_expr.data_type(&input_schema)?,
                        physical_expr.nullable(&input_schema)?,
                    );
                    
                    physical_exprs.push((physical_expr, name));
                    output_fields.push(field);
                }
                
                let output_schema = Arc::new(arrow::datatypes::Schema::new(output_fields));
                let projection_exec = ProjectionExec::new(input, physical_exprs, output_schema);
                Ok(Arc::new(PhysicalPlan::Projection(projection_exec)))
            }
            
            LogicalPlan::Filter(filter) => {
                let input = self.create_physical_plan(&filter.input).await?;
                let input_schema = input.schema();
                
                let predicate = create_physical_expr(
                    &filter.predicate,
                    &input_schema,
                    &self.session_ctx.state().execution_props(),
                )?;
                
                let filter_exec = FilterExec::new(input, predicate);
                Ok(Arc::new(PhysicalPlan::Filter(filter_exec)))
            }
            
            LogicalPlan::Join(join) => {
                let left = self.create_physical_plan(&join.left).await?;
                let right = self.create_physical_plan(&join.right).await?;
                
                let left_schema = left.schema();
                let right_schema = right.schema();
                
                let mut left_keys = Vec::new();
                let mut right_keys = Vec::new();
                
                for (left_expr, right_expr) in &join.on {
                    let left_key = create_physical_expr(
                        left_expr,
                        &left_schema,
                        &self.session_ctx.state().execution_props(),
                    )?;
                    let right_key = create_physical_expr(
                        right_expr,
                        &right_schema,
                        &self.session_ctx.state().execution_props(),
                    )?;
                    
                    left_keys.push(left_key);
                    right_keys.push(right_key);
                }
                
                let join_type = match join.join_type {
                    LogicalJoinType::Inner => crate::operators::hash_join::JoinType::Inner,
                    LogicalJoinType::Left => crate::operators::hash_join::JoinType::Left,
                    LogicalJoinType::Right => crate::operators::hash_join::JoinType::Right,
                    LogicalJoinType::Full => crate::operators::hash_join::JoinType::Full,
                    _ => return Err(DataFusionError::NotImplemented(
                        format!("Join type {:?} not implemented", join.join_type)
                    )),
                };
                
                let hash_join = HashJoinExec::new(left, right, left_keys, right_keys, join_type)?;
                Ok(Arc::new(PhysicalPlan::HashJoin(hash_join)))
            }
            
            _ => Err(DataFusionError::NotImplemented(
                format!("Logical plan {:?} not implemented", logical_plan)
            )),
        }
    }
}