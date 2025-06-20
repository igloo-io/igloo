# 🍙 Igloo: The Distributed SQL Query Engine

Igloo is a high-performance, distributed SQL query engine built in Rust. It is designed from the ground up to query data from a multitude of sources, including operational databases, data lakes, and streaming systems. Igloo provides a single, unified SQL interface to your entire data ecosystem.

Our mission is to make data access simple, fast, and intelligent. By leveraging a state-of-the-art query engine and a modern data transport protocol, Igloo dramatically accelerates data science and analytics workloads.

## Core Features
- **Federated Queries**: Connect to multiple, disparate data sources (like PostgreSQL, MySQL, and data lake files) and query them together in a single SQL statement.
- **Powered by Apache Arrow DataFusion**: Igloo is built on the lightning-fast, extensible, and Rust-native Apache Arrow DataFusion query engine. This provides a rich set of optimizations and a powerful foundation for analytics.
- **High-Speed Data Transport with Apache Arrow Flight SQL**: Igloo uses Apache Arrow Flight SQL for client-server communication, a modern protocol that is significantly faster than traditional methods like ODBC and JDBC.
- **Intelligent Caching**: Igloo features a transparent caching layer. Frequently accessed data is automatically cached and kept up-to-date, providing orders-of-magnitude speedups for repeated queries.
- **Elastic & Scalable**: Igloo is designed to run on a cluster of machines, distributing query processing seamlessly across all available resources. It can scale from a single laptop to thousands of nodes.
- **Modern & Safe**: Written in Rust, Igloo guarantees memory safety and concurrency, eliminating common bugs found in distributed systems and ensuring high reliability.
- **Extensible by Design**: The "Connector" architecture makes it trivial to add support for new data sources.

## 🚀 Quickstart
### Setup Your Environment:
Run the setup script to install all necessary dependencies and tools.

```bash
./scripts/setup.sh
```

### Build the Project:
Build all components using the main build script.

```bash
./scripts/build.sh
```

### Run the System:
#### Start the Coordinator Node:
```bash
cargo run --bin igloo-coordinator
```
The coordinator will start, register a sample CSV file as `test_table`, and listen for client connections on `127.0.0.1:50051`.

#### Start a Worker Node (in a new terminal):
```bash
cargo run --bin igloo-worker
```
The worker will start, register itself with the coordinator, and begin sending heartbeats.

### Run Tests:
Execute the entire test suite to ensure everything is working correctly.

```bash
cargo test --workspace
```

## Architecture Overview
Igloo's architecture is simple and robust, consisting of two primary types of nodes: a single Coordinator and multiple Workers.

### The Coordinator Node
The Coordinator is the brain of the Igloo cluster. It is a single process responsible for:
- **Accepting Client Connections**: Data scientists and applications connect to the Coordinator via the Arrow Flight SQL endpoint to submit SQL queries.
- **Query Planning**: It leverages Apache Arrow DataFusion to parse, plan, and optimize the SQL query. This includes deciding whether to query a live database or use a faster, cached data source.
- **Cluster Management & Scheduling**: The Coordinator maintains a real-time view of all Worker nodes in the cluster. It breaks the query plan into smaller, parallelizable execution tasks and assigns them to the available workers.

### The Worker Nodes
The Workers are the hands of the cluster. Each worker node runs a single igloo-worker process responsible for:
- **Registering with the Coordinator**: On startup, a worker announces its presence to the Coordinator.
- **Executing Tasks**: The worker accepts execution tasks from the Coordinator.
- **Fetching Data**: It uses a specific Connector to fetch the data required for its task—either from an external database or the local filesystem.
- **Processing Data**: It processes the data in memory using Igloo's high-performance query engine. This can involve filtering, joining, or aggregating data.
- **Communicating Results**: It can send intermediate results to other workers for further processing or send final results back to the Coordinator.

## Repository Structure
This project is a Cargo workspace, making it easy to manage multiple interconnected packages. Here's a guide to the most important directories:
- **/api**: Contains the .proto definitions for all network communication and the generated Rust code for the FlightService.
- **/crates**: Contains all the core Rust source code for Igloo.
  - **igloo-coordinator**: Source code for the Coordinator node. The "brain".
  - **igloo-worker**: Source code for the Worker nodes. The "hands".
  - **igloo-engine**: The core, non-distributed query processing logic, powered by DataFusion.
  - **igloo-cache**: The library for reading from and writing to our cache.
  - **igloo-common**: Shared types, errors, and catalog functionality used across the workspace.
  - **connectors/**: A home for all data source plugins, such as the filesystem connector.
- **/pyigloo**: Python bindings to make it easy to query Igloo from tools like Jupyter and Pandas.
- **/scripts**: Helper scripts for setup, building, validation, and formatting.

## Contributing
We welcome contributions of all kinds, from bug reports to feature enhancements. Please read our Contributing Guide to get started. The project roadmap is also available in `roadmap.md`.
