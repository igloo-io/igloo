# Igloo Project Roadmap

This document outlines the development roadmap for Igloo, our distributed SQL query engine. Our goal is to build a high-performance, scalable, and user-friendly platform for data analytics. This roadmap is a living document and will evolve as we make progress and receive feedback from the community.

---

## Phase 1: Next-Generation Core Engine & Transport Layer Upgrade

The primary focus of this phase is to replace the foundational components of Igloo with best-in-class, open-source technologies. This will provide a massive leap in performance, reduce our maintenance burden, and allow us to focus on higher-level features.

* **Integrate Apache Arrow DataFusion as the Core Query Engine:**
    * **Description:** We will replace our custom `igloo-engine` components with Apache Arrow DataFusion.  DataFusion is a powerful and extensible query engine that will provide us with a state-of-the-art planner, optimizer, and execution runtime.
    * **Key Tasks:**
        * [✅] Replace `logical_plan.rs` and `physical_plan.rs` with DataFusion's `LogicalPlan` and `ExecutionPlan`.
        * [✅] Use DataFusion's SQL parser and planner to handle incoming queries.
        * [ ] Implement a custom `TableProvider` for our caching layer to allow DataFusion to query it directly.
        * [ ] Benchmark the new engine against the old one to quantify performance improvements.

* **Implement Apache Arrow Flight SQL for Data Transport:**
    * **Description:** We will replace our current gRPC-based communication with Apache Arrow Flight SQL. This will dramatically improve data transfer speeds between the client, coordinator, and workers, and reduce serialization overhead.
    * **Key Tasks:**
        * [✅] Define a `FlightSqlService` in the `api` crate that implements the `FlightService` trait.
        * [ ] Implement the Flight SQL endpoints (`get_flight_info`, `do_get`, etc.) in the `igloo-coordinator`.
        * [ ] Update the `igloo-client` to use an Arrow Flight SQL client.
        * [ ] Develop new Python bindings that use the `arrow-flight-sql-client` library.

---

## Phase 2: Advanced Caching and Storage Offloading

With a new engine and transport layer in place, this phase will focus on making our caching and storage layers more intelligent and efficient. The goal is to minimize data movement and push computation as close to the data as possible.

* **Skyhook-Inspired Caching Layer:**
    * **Description:** Inspired by the "Skyhook" paper, we will enhance our caching layer to support predicate and projection pushdown directly into the cache files.  This will reduce the amount of data that needs to be read from disk and sent over the network.
    * **Key Tasks:**
        * [ ] Standardize the cache file format on **Parquet** to leverage its metadata and predicate pushdown capabilities.
        * [ ] Enhance the `igloo-worker` to apply filters and projections directly when reading cache files.
        * [ ] Update the coordinator's planner to identify and push down parts of the query to the workers.

* **Zero-Copy and Memory Efficiency:**
    * **Description:** We will focus on minimizing data copies within and between our components, drawing inspiration from the "Zerrow" paper. 
    * **Key Tasks:**
        * [ ] Use the Arrow IPC format for all data transfers between the coordinator and workers.
        * [ ] Investigate the use of shared memory for inter-process communication on the same node.
        * [ ] Profile and optimize string handling and memory allocations, as suggested by the "ConnectorX" paper. 

---

## Phase 3: Ecosystem, Usability, and Production Readiness

This phase is about making Igloo easy to use, secure, and ready for production workloads.

* **Robust Python Bindings:**
    * **Description:** Create a seamless and intuitive experience for Python users.
    * **Key Tasks:**
        * [ ] Ensure the Python client is easy to install and use.
        * [ ] Provide helper functions for converting Arrow data to Pandas and Polars DataFrames.
        * [ ] Write comprehensive documentation and examples for the Python library.

* **Security and Authentication:**
    * **Description:** Implement essential security features for a distributed system.
    * **Key Tasks:**
        * [ ] Enable TLS encryption for all Arrow Flight SQL communication.
        * [ ] Add support for authentication and authorization.

* **Pluggable Connectors:**
    * **Description:** Expand our library of connectors to support a wider range of data sources.
    * **Key Tasks:**
        * [ ] Add connectors for popular NoSQL databases (e.g., MongoDB, Cassandra).
        * [ ] Add connectors for cloud data warehouses (e.g., BigQuery, Snowflake).

* **Comprehensive Documentation and Examples:**
    * **Description:** Improve our documentation to make it easier for new users to get started and for developers to contribute.
    * **Key Tasks:**
        * [ ] Write in-depth tutorials and guides.
        * [ ] Create a gallery of examples showcasing different use cases.
        * [ ] Document the internal architecture and design decisions.