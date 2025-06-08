# Igloo: The Distributed SQL Query Engine

![Igloo Logo Placeholder](https://placehold.co/600x300/172554/ffffff?text=Igloo)

---

## 🚀 Quickstart (For Beginners)

1. **Install Prerequisites:**
   - [Rust (latest stable)](https://www.rust-lang.org/tools/install)
   - [Protocol Buffers Compiler (`protoc`)](https://grpc.io/docs/protoc-installation/)
   - (Optional) [Python 3.x](https://www.python.org/downloads/) if you want to use Python bindings
2. **Clone the repository:**
   ```bash
   git clone <your-fork-or-this-repo-url>
   cd igloo
   ```
3. **Build everything:**
   ```bash
   cargo build
   ```
4. **Run tests:**
   ```bash
   cargo test
   ```
5. **Format code:**
   ```bash
   cargo fmt
   ```

---

Igloo is a high-performance, distributed SQL query engine built in Rust. It is designed from the ground up to query data from a multitude of sources, including operational databases, data lakes, and streaming systems. Igloo provides a single, unified SQL interface to your entire data ecosystem.

Our mission is to make data access simple, fast, and intelligent. By leveraging an advanced caching layer and a modern, parallel execution engine, Igloo dramatically accelerates data science and analytics workloads.

# Igloo Project Structure

This repository is a Cargo workspace for the Igloo distributed SQL query engine. All core components are implemented as Rust crates under `/crates` and `/api`. Python bindings are under `/pyigloo` (Rust crate) and `/python/pyigloo` (Python package).

## Building the Workspace

To build all components:

```bash
cargo build
```

---

### Core Features

* **Federated Queries**: Connect to multiple, disparate data sources (like PostgreSQL, MySQL, and data lake files) and query them together in a single SQL statement.
* **High-Performance Execution**: Built on Apache Arrow, Igloo utilizes a columnar, in-memory format for all data processing, minimizing serialization overhead and maximizing the benefits of modern CPU architectures.
* **Intelligent Caching**: Igloo features a transparent caching layer built on Apache Iceberg. Frequently accessed data is automatically cached and kept up-to-date, providing orders-of-magnitude speedups for repeated queries.
* **Elastic & Scalable**: Igloo is designed to run on a cluster of machines, distributing query processing seamlessly across all available resources. It can scale from a single laptop to thousands of nodes.
* **Modern & Safe**: Written in Rust, Igloo guarantees memory safety and concurrency, eliminating common bugs found in distributed systems and ensuring high reliability.
* **Extensible by Design**: The "Connector" architecture makes it trivial to add support for new data sources.

---

### Architecture Overview

Igloo's architecture is simple and robust, consisting of two primary types of nodes: a single **Coordinator** and multiple **Workers**.

![Igloo Architecture Diagram](https://placehold.co/800x450/e0f2fe/172554?text=Coordinator-Worker%20Architecture)

#### The Coordinator Node

The Coordinator is the brain of the Igloo cluster. It is a single process responsible for:

1.  **Accepting Client Connections**: Data scientists and applications connect to the Coordinator to submit SQL queries.
2.  **Query Planning**: It parses the SQL and creates a an efficient, optimized plan to fetch and process the data. This includes a crucial step where it decides whether to query a live database or use the faster, cached data.
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
2.  The **Coordinator** receives the query. It checks its catalog and sees that `postgres_orders` is backed by a frequently updated cache.
3.  The **Planner** inside the Coordinator rewrites the query to use the cache. It creates a physical plan, which is a DAG (Directed Acyclic Graph) of operations.
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
    * `igloo-engine`: The core, non-distributed query processing logic. This is where SQL rules and execution operators live.
    * `igloo-cache`: The library for reading from and writing to our Iceberg cache.
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
```

---

### Running Tests

To run all tests for all crates:

```bash
cargo test
```

If you want to run tests for a specific crate:

```bash
cd crates/engine
cargo test
```

---

### Code Style & Linting

- Format code using [rustfmt](https://github.com/rust-lang/rustfmt):
  ```bash
  cargo fmt
  ```
- Lint code using [clippy](https://github.com/rust-lang/rust-clippy):
  ```bash
  cargo clippy
  ```
- The configuration files `rustfmt.toml` and `clippy.toml` are provided in the root directory.

---

### GitHub Actions (CI)

This repository uses GitHub Actions to automatically build and test all code on every pull request and push. You can view the status of the latest builds in the "Actions" tab on GitHub. If your PR fails CI, click on the red ❌ to see the error log and fix the issues before merging.

---

### Troubleshooting

- **Build fails with missing dependencies:**
  - Make sure you have installed all prerequisites (see Quickstart).
  - Run `rustup update` to ensure your Rust toolchain is up to date.
- **`protoc` not found:**
  - Install Protocol Buffers Compiler and ensure it is in your PATH.
- **Tests fail:**
  - Read the error message carefully. If you are stuck, ask for help (see below).

---

### How to Ask for Help

- If you are stuck, first search the error message online.
- If you still need help, open an issue or ask in the team chat. Please include:
  - What you tried
  - The error message
  - Your OS and Rust version

---

## Roadmap
- [ ] Core engine improvements
- [ ] Pluggable connectors
- [ ] Security and authentication
- [ ] Python bindings
- [ ] Performance optimizations

## Contributing Ideas
- Add new connectors (e.g., for more databases)
- Improve documentation and examples
- Add more tests and CI jobs
- Suggest new features via issues or pull requests
