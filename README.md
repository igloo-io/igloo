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

3. **Quick Start with Docker:**
   ```bash
   docker-compose up -d --build
   ```

4. **Run Tests:**
   ```bash
   cargo test
   ```

---

## ✨ Core Features

* **🔗 Federated Queries**: Connect multiple data sources (PostgreSQL, MySQL, data lakes) and query them together in a single SQL statement
* **⚡ Powered by Apache Arrow DataFusion**: Lightning-fast, extensible Rust-native query engine with rich optimizations
* **🚀 High-Speed Transport**: Apache Arrow Flight SQL for client-server communication—significantly faster than ODBC/JDBC
* **🧠 Intelligent Caching**: Transparent caching layer with automatic cache invalidation via Change Data Capture (CDC)
* **📈 Elastic & Scalable**: Distributed architecture that scales from a single laptop to thousands of nodes
* **🛡️ Memory Safe**: Written in Rust for guaranteed memory safety and high reliability
* **🔌 Extensible**: Modular connector architecture makes adding new data sources trivial

---

## 🏗️ Architecture Overview

Igloo uses a simple yet powerful coordinator-worker architecture:

### 🧠 The Coordinator Node
The brain of the cluster, responsible for:
- **Client Connections**: Arrow Flight SQL endpoint for SQL query submission
- **Query Planning**: Apache Arrow DataFusion-powered SQL parsing, planning, and optimization
- **Smart Routing**: Decides between live database queries and cached data
- **Cluster Management**: Real-time worker tracking and intelligent task scheduling

### 👷 Worker Nodes
The hands of the cluster, each worker:
- **Registers** with the Coordinator announcing available resources
- **Executes** assigned query tasks using specialized connectors
- **Processes** data in-memory with high-performance query engine
- **Communicates** results between workers and back to the Coordinator

### 🔄 Query Execution Flow

```mermaid
sequenceDiagram
    participant Client
    participant Coordinator
    participant Worker1
    participant Worker2
    participant Cache/DB

    Client->>Coordinator: SQL Query via Flight SQL
    Coordinator->>Coordinator: Parse & Plan (DataFusion)
    Coordinator->>Coordinator: Check Cache Strategy
    
    par Distributed Execution
        Coordinator->>Worker1: Task 1: Scan data partition A
        Coordinator->>Worker2: Task 2: Scan data partition B
    end
    
    par Data Fetching
        Worker1->>Cache/DB: Fetch partition A
        Worker2->>Cache/DB: Fetch partition B
    end
    
    Worker1->>Coordinator: Results A
    Worker2->>Coordinator: Results B
    Coordinator->>Coordinator: Combine & Finialize
    Coordinator->>Client: Final Results
```

**Example Query Execution:**
1. User submits: `SELECT * FROM postgres_orders WHERE region = 'EMEA'`
2. Coordinator checks catalog—discovers cached, up-to-date data available
3. DataFusion planner creates optimized physical execution plan
4. Scheduler distributes scan tasks across available workers
5. Workers execute in parallel, filtering for 'EMEA' region
6. Results stream back through Coordinator to client

---

## 📁 Repository Structure

```
igloo/
├── 📡 api/                    # Protocol Buffers definitions
├── 🦀 crates/                 # Core Rust packages
│   ├── igloo-coordinator/     # 🧠 Coordinator node logic
│   ├── igloo-worker/          # 👷 Worker node implementation  
│   ├── igloo-engine/          # ⚙️ Core query processing (DataFusion)
│   ├── igloo-cache/           # 💾 Caching layer
│   └── connectors/            # 🔌 Data source plugins
├── 🐍 python/                 # Python bindings
├── 📚 docs/                   # Documentation & design decisions
└── 💡 examples/               # Sample code & tutorials
```

---

## 🚀 Getting Started

### Option 1: Docker Compose (Recommended)

The easiest way to get Igloo running with all dependencies:

```bash
# Start Igloo cluster with PostgreSQL
docker-compose up -d --build

# View logs
docker-compose logs -f igloo

# Stop services
docker-compose down
```

### Option 2: Local Development

For development and customization:

1. **Prerequisites:**
   * Rust toolchain (see `rust-toolchain.toml`)
   * Protocol Buffers Compiler (`protoc`)
   * Running PostgreSQL instance
   * ADBC drivers (see environment configuration below)

2. **Configure Environment:**
   ```bash
   cp .env.example .env
   # Edit .env with your database connections and paths
   ```

3. **Build and Run:**
   ```bash
   cargo build --release
   cargo run
   ```

---

## ⚙️ Configuration

Igloo is configured via environment variables. Copy `.env.example` to `.env` for local development.

### 🔗 Database Connections
```bash
# Primary PostgreSQL connection
DATABASE_URL=postgres://user:password@localhost:5432/mydb

# Alternative format
IGLOO_POSTGRES_URI=host=localhost user=postgres password=postgres dbname=mydb
```

### 📂 Data Paths
```bash
# Parquet/Iceberg data location
IGLOO_PARQUET_PATH=./dummy_iceberg_cdc/

# CDC monitoring path
IGLOO_CDC_PATH=./dummy_iceberg_cdc/
```

### 🔧 ADBC Drivers (Local Development)
```bash
# Required for local execution (not needed in Docker)
export LD_LIBRARY_PATH=/path/to/adbc/drivers:$LD_LIBRARY_PATH

# For integration tests
TEST_ADBC_POSTGRESQL_URI=postgresql://user:password@localhost:5432/test_db
```

---

## 💻 Example Usage

### Rust API
```rust
use igloo::{Coordinator, WorkerConfig};
use tokio;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Start coordinator
    let coordinator = Coordinator::new("0.0.0.0:50051").await?;
    
    // Execute federated query
    let results = coordinator.execute_sql(
        "SELECT o.order_id, c.customer_name 
         FROM postgres_orders o 
         JOIN lakehouse_customers c ON o.customer_id = c.id 
         WHERE o.created_at > '2024-01-01'"
    ).await?;
    
    println!("Query results: {:?}", results);
    Ok(())
}
```

### Python Bindings
```python
import igloo

# Connect to Igloo cluster
client = igloo.connect("grpc://localhost:50051")

# Execute SQL with automatic caching
df = client.sql("""
    SELECT region, SUM(revenue) as total_revenue
    FROM sales_data 
    WHERE date >= '2024-01-01'
    GROUP BY region
    ORDER BY total_revenue DESC
""")

print(df.to_pandas())
```

---

## 🎯 Current Features

* ⚡ **Fast SQL Execution** with Apache DataFusion
* 🍙 **Distributed Processing** across multiple nodes  
* 🧊 **Smart Result Caching** with query fingerprinting
* 🔄 **CDC-Driven Cache Invalidation** from Iceberg change logs
* 🔗 **Cross-Source Joins** between PostgreSQL and Arrow datasets
* 🛡️ **Memory Safety** guaranteed by Rust
* 📊 **Arrow Flight SQL** for high-performance client communication

---

## 🛤️ Roadmap

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

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 🍙 About Igloo

Igloo makes data access simple by bridging the gap between operational databases and analytical workloads. Built by developers who understand the pain of slow, complex data pipelines, Igloo provides the performance and simplicity your team needs to focus on insights, not infrastructure.

**Star ⭐ this repository if Igloo helps power your data journey!**
