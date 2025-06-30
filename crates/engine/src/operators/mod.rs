//! Physical operators for query execution

pub mod parquet_scan;
pub mod projection;
pub mod filter;
pub mod hash_join;

pub use parquet_scan::ParquetScanExec;
pub use projection::ProjectionExec;
pub use filter::FilterExec;
pub use hash_join::HashJoinExec;