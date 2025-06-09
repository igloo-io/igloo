# Igloo: The Distributed SQL Query Engine

![Igloo Logo Placeholder](https://placehold.co/400x200/172554/ffffff?text=Igloo)

---

## 🚀 Quickstart

1.  **Install Prerequisites:**
    * [Rust (latest stable)](https://www.rust-lang.org/tools/install)
    * [Protocol Buffers Compiler (`protoc`)](https://grpc.io/docs/protoc-installation/)
    * (Optional) [Python 3.x](https://www.python.org/downloads/) if you want to use Python bindings
2.  **Clone the repository:**
    ```bash
    git clone https://github.com/igloo-io/igloo
    cd igloo
    ```
3.  **Build everything:**
    ```bash
    cargo build
    ```
4.  **Run tests:**
    ```bash
    cargo test
    ```
5.  **Format code:**
    ```bash
    cargo fmt
    ```

---

Igloo is a high-performance, distributed SQL query engine built in Rust. It is designed from the ground up to query data from a multitude of sources, including operational databases, data lakes, and streaming systems. Igloo provides a single, unified SQL interface to your entire data ecosystem.

Our mission is to make data access simple, fast, and intelligent. By leveraging a state-of-the-art query engine and a modern data transport protocol, Igloo dramatically accelerates data science and analytics workloads.

## Core Features

* **Federated Queries**: Connect to multiple, disparate data sources (like PostgreSQL, MySQL, and data lake files) and query them together in a single SQL statement.
* [cite_start]**Powered by Apache Arrow DataFusion**: Igloo is built on the lightning-fast, extensible, and Rust-native **Apache Arrow DataFusion** query engine.  This provides a rich set of optimizations and a powerful foundation for analytics.
* **High-Speed Data Transport with Apache Arrow Flight SQL**: Igloo uses **Apache Arrow Flight SQL** for client-server communication, a modern protocol that is significantly faster than traditional methods like ODBC and JDBC.
* **Intelligent Caching**: Igloo features a transparent caching layer. Frequently accessed data is automatically cached and kept up-to-date, providing orders-of-magnitude speedups for repeated queries.
* **Elastic & Scalable**: Igloo is designed to run on a cluster of machines, distributing query processing seamlessly across all available resources. It can scale from a single laptop to thousands of nodes.
* **Modern & Safe**: Written in Rust, Igloo guarantees memory safety and concurrency, eliminating common bugs found in distributed systems and ensuring high reliability.
* **Extensible by Design**: The "Connector" architecture makes it trivial to add support for new data sources.

---

### Architecture Overview

Igloo's architecture is simple and robust, consisting of two primary types of nodes: a single **Coordinator** and multiple **Workers**.

![Igloo Architecture Diagram](https://placehold.co/800x450/e0f2fe/172554?text=Coordinator-Worker%20Architecture)

#### The Coordinator Node

The Coordinator is the brain of the Igloo cluster. It is a single process responsible for:

1.  **Accepting Client Connections**: Data scientists and applications connect to the Coordinator via the Arrow Flight SQL endpoint to submit SQL queries.
2.  **Query Planning**: It leverages Apache Arrow DataFusion to parse, plan, and optimize the SQL query. This includes a crucial step where it decides whether to query a live database or use the faster, cached data.
3.  **Cluster Management & Scheduling**: The Coordinator maintains a real-time view of all Worker nodes in the cluster. It knows which workers are available and what resources they have. It breaks the query plan into smaller, parallelizable **execution tasks** and assigns them to the available workers.

#### The Worker Nodes

The Workers are the hands of the cluster. Each worker node runs a single `igloo-worker` process. This process is responsible for:

1.  **Registering with the Coordinator**: On startup, a worker announces its presence and available resources (CPU, memory) to the Coordinator.
2.  **Executing Tasks**: The worker accepts execution tasks from the Coordinator.
3.  **Fetching Data**: It uses a specific **Connector** to fetch the data required for its task—either from an external database or the local filesystem.
4.  **Processing Data**: It processes the data in memory using Igloo's high-performance query engine. This can involve filtering, joining, or aggregating data.
5.  **Communicating Results**: It can send intermediate results to other workers for further processing or send final results back to the Coordinator.

#### How a Query is Executed

1.  A user sends a SQL query: `SELECT * FROM postgres_orders WHERE region = 'EMEA';`
2.  The **Coordinator** receives the query via its Arrow Flight SQL endpoint. It checks its catalog and sees that `postgres_orders` is backed by a frequently updated cache.
3.  The **DataFusion Planner** inside the Coordinator rewrites the query to use the cache. It creates a physical plan, which is a DAG (Directed Acyclic Graph) of operations.
4.  The **Scheduler** inside the Coordinator breaks the plan into tasks. For example: *Task 1: Scan 10 cache files. Task 2: Scan another 10 cache files.* It sends these tasks to two different idle **Workers**.
5.  Each **Worker** executes its task. It reads the specified files from the cache, filters for the 'EMEA' region, and produces an Arrow `RecordBatch` in memory.
6.  The results are streamed back to the Coordinator, which then forwards them to the user.

---

### Repository Structure

This project is a Cargo workspace, making it easy to manage multiple interconnected packages. Here's a guide to the most important directories:

* `/api`: Contains the `.proto` definitions for all network communication. This is the blueprint for how our distributed components talk to each other.
* `/crates`: Contains all the core Rust source code for Igloo.
    * `igloo-coordinator`: Source code for the Coordinator node. The "brain".
    * `igloo-worker`: Source code for the Worker nodes. The "hands".
    * `igloo-engine`: The core, non-distributed query processing logic, now powered by DataFusion. This is where SQL rules and execution operators live.
    * `igloo-cache`: The library for reading from and writing to our cache.
    * `connectors/`: A home for all data source plugins. Adding a new database connection starts here!
* `/python`: Python bindings to make it easy to query Igloo from tools like Jupyter, Pandas, and Polars.
* `/docs`: In-depth documentation and design decision records.
* `/examples`: Sample code to help you get started and learn how to use Igloo.

---

### Getting Started

#### Prerequisites

* Rust (latest stable version, see `rust-toolchain.toml`)
* Protocol Buffers Compiler (`protoc`)

#### Building the Project

Because this is a Cargo workspace, you can build all the components from the root directory:

```bash
# Build all crates in development mode
cargo build

# Build all crates in release mode (for performance)
cargo build --release