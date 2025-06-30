# 🍙 Igloo: The Distributed SQL Query Engine

Igloo is a high-performance, distributed SQL query engine built in Rust that makes data access simple, fast, and intelligent. Query data from multiple sources—operational databases, data lakes, and streaming systems—through a single, unified SQL interface.

---

## 🚀 Quickstart

1. **Install Prerequisites:**
   * [Rust (latest stable)](https://www.rust-lang.org/tools/install)
   * [Protocol Buffers Compiler (`protoc`)](https://grpc.io/docs/protoc-installation/)
   * (Optional) [Python 3.x](https://www.python.org/downloads/) for Python bindings

2. **Clone and Build:**
   ```bash
   git clone https://github.com/igloo-io/igloo
   cd igloo
   cargo build --release
   ```

3. **Quick Start with Sample Query:**
   ```bash
   # Run a simple local query
   cargo run --bin igloo -- --sql "SELECT 42 as answer, 'Hello Igloo' as message"
   
   # Run a query against sample data
   cargo run --bin igloo -- --sql "SELECT name, age FROM users WHERE age > 25"
   ```

4. **Run Tests:**
   ```bash
   cargo test
   ```

---

## ✨ Core Features

* **🔗 Federated Queries**: Connect multiple data sources (PostgreSQL, MySQL, Parquet, Iceberg) and query them together
* **⚡ Powered by Apache Arrow DataFusion**: Lightning-fast, extensible Rust-native query engine with rich optimizations
* **🚀 High-Speed Transport**: Apache Arrow Flight SQL for client-server communication—significantly faster than ODBC/JDBC
* **🧠 Intelligent Caching**: Transparent caching layer with automatic cache invalidation via Change Data Capture (CDC)
* **📈 Elastic & Scalable**: Distributed architecture that scales from a single laptop to thousands of nodes
* **🛡️ Memory Safe**: Written in Rust for guaranteed memory safety and high reliability
* **🔌 Extensible**: Modular connector architecture makes adding new data sources trivial

---

## 🏗️ Architecture Overview

Igloo uses a coordinator-worker architecture with a sophisticated physical execution engine:

### 🧠 Query Engine Core
- **Physical Operators**: Optimized operators for scans, projections, filters, and hash joins
- **Physical Planner**: Converts logical plans to executable physical plans
- **Execution Engine**: Streams data through operator pipelines with minimal memory overhead

### 🧠 The Coordinator Node
The brain of the cluster, responsible for:
- **Client Connections**: Arrow Flight SQL endpoint for SQL query submission
- **Query Planning**: Apache Arrow DataFusion-powered SQL parsing, planning, and optimization
- **Fragment Generation**: Breaks complex queries into distributable fragments
- **Cluster Management**: Real-time worker tracking and intelligent task scheduling

### 👷 Worker Nodes
The hands of the cluster, each worker:
- **Registers** with the Coordinator announcing available resources
- **Executes** assigned query fragments using specialized connectors
- **Processes** data in-memory with high-performance query engine
- **Communicates** results between workers and back to the Coordinator

---

## 📁 Repository Structure

```
igloo/
├── 📡 crates/api/              # Protocol Buffers & gRPC definitions
├── 🦀 crates/engine/           # ⚙️ Core query processing engine
│   ├── physical_plan.rs        # Physical plan representation
│   ├── physical_planner.rs     # Logical to physical plan conversion
│   └── operators/              # Physical operators (scan, join, etc.)
├── 🧠 crates/coordinator/      # Coordinator node logic
├── 👷 crates/worker/           # Worker node implementation  
├── 🔌 crates/connectors/       # Data source plugins
│   ├── filesystem/             # Parquet/CSV file connector
│   ├── iceberg/               # Apache Iceberg connector
│   ├── postgres/              # PostgreSQL connector
│   └── mysql/                 # MySQL connector
├── 💾 crates/cache/           # Caching layer
├── 🐍 pyigloo/               # Python bindings
└── 📚 docs/                  # Documentation & design decisions
```

---

## 🚀 Getting Started

### Local Development

1. **Build the project:**
   ```bash
   cargo build --release
   ```

2. **Run sample queries:**
   ```bash
   # Simple query
   cargo run --bin igloo -- --sql "SELECT 42 as answer"
   
   # Query with sample data
   cargo run --bin igloo -- --sql "SELECT name FROM users WHERE age > 30"
   ```

3. **Run tests:**
   ```bash
   # Unit tests
   cargo test
   
   # Integration tests
   cargo test --test integration_test
   ```

### Distributed Mode (Coming Soon)

```bash
# Start coordinator
cargo run --bin igloo-coordinator

# Start workers
cargo run --bin igloo-worker -- --coordinator http://localhost:50051

# Execute distributed query
cargo run --bin igloo -- --distributed --sql "SELECT * FROM large_table"
```

---

## 💻 Example Usage

### Rust API
```rust
use igloo_engine::QueryEngine;
use datafusion::arrow::util::pretty::print_batches;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let engine = QueryEngine::new();
    
    // Execute SQL query
    let results = engine.execute("SELECT 42 as answer").await;
    
    // Print results
    print_batches(&results)?;
    
    Ok(())
}
```

### Physical Plan Execution
```rust
use igloo_engine::{PhysicalPlanner, QueryEngine};
use datafusion::execution::context::SessionContext;
use igloo_common::catalog::MemoryCatalog;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let catalog = Arc::new(MemoryCatalog::new());
    let session_ctx = Arc::new(SessionContext::new());
    let planner = PhysicalPlanner::new(catalog, session_ctx.clone());
    
    // Parse SQL to logical plan
    let logical_plan = session_ctx.sql("SELECT * FROM my_table").await?.into_optimized_plan()?;
    
    // Convert to physical plan
    let physical_plan = planner.create_physical_plan(&logical_plan).await?;
    
    // Execute and stream results
    let mut stream = physical_plan.execute().await?;
    while let Some(batch) = stream.next().await {
        println!("Batch: {:?}", batch?);
    }
    
    Ok(())
}
```

---

## 🎯 Current Features

### Phase 1: Single-Node Execution ✅
* ⚡ **Physical Operators**: ParquetScan, Projection, Filter, HashJoin
* 🧠 **Physical Planner**: Converts DataFusion logical plans to executable physical plans
* 🔄 **Streaming Execution**: Memory-efficient streaming of query results
* 📊 **Arrow Integration**: Native Arrow RecordBatch processing

### Phase 2: Distributed Execution 🚧
* 🌐 **Query Fragmentation**: Break queries into distributable fragments
* 📡 **gRPC Communication**: Coordinator-worker communication via Protocol Buffers
* 🔗 **Cross-Source Joins**: Distributed joins between different data sources
* 🧊 **Iceberg Support**: Read from Apache Iceberg tables

---

## 🛤️ Roadmap

### Immediate (Current Sprint)
- [x] 🏗️ **Physical Operators** (Scan, Project, Filter, Join)
- [x] 🧠 **Physical Planner** 
- [x] ⚡ **Single-Node Execution Engine**
- [ ] 🌐 **Distributed Query Fragments**
- [ ] 📡 **Worker Communication Protocol**
- [ ] 🧊 **Iceberg Connector**

### Near Term
- [ ] 🌐 **REST API** for easier client integration
- [ ] ⏱️ **Async CDC Updates** with live cache refresh  
- [ ] 📊 **Query Metrics** (Prometheus, OpenTelemetry)
- [ ] 🔧 **Enhanced Connector Framework**

### Future Vision  
- [ ] 🧠 **ML-Powered Query Optimization**
- [ ] 🌍 **Multi-Region Deployments**
- [ ] 📦 **Persistent Cache Backends** (RocksDB, Redis)
- [ ] 🔐 **Advanced Security & Auth**
- [ ] 📈 **Auto-scaling** based on query patterns

---

## 🧪 Testing

Run the comprehensive test suite:

```bash
# All tests
cargo test

# Engine tests only
cargo test -p igloo-engine

# Integration tests
cargo test --test integration_test

# With output
cargo test -- --nocapture
```

---

## 🤝 Contributing

We welcome contributions! Whether you're fixing bugs, adding features, or improving documentation:

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/amazing-feature`
3. Make your changes and add tests
4. Run the test suite: `cargo test`
5. Submit a pull request

See [CONTRIBUTING.md](CONTRIBUTING.md) for detailed guidelines.

---

## 📄 License

This project is licensed under the GNU AGPLv3 License - see the [LICENSE](LICENSE) file for details.

---

## 🍙 About Igloo

Igloo makes data access simple by bridging the gap between operational databases and analytical workloads. Built by developers who understand the pain of slow, complex data pipelines, Igloo provides the performance and simplicity your team needs to focus on insights, not infrastructure.

**Star ⭐ this repository if Igloo helps power your data journey!**