# Test Data for Iceberg Connector

This directory contains test data for the Iceberg connector. The data includes:
1.  A simple Parquet file (`table_a.parquet`).
2.  An Iceberg table (`iceberg_table/`) representing `table_b`, which includes Parquet data file(s) and associated metadata.

## Parquet Data Generation

The Parquet files (`table_a.parquet` and data for `table_b`) are generated using the Python script `generate_parquet_data.py`.

### Dependencies

To run the script, you need Python with the following packages installed:
*   `pyarrow==20.0.0` (or a compatible version)

You can install it using pip:
```bash
pip install pyarrow
# Or, if using the project's virtual environment:
# source pyigloo/.venv/bin/activate
# pip install pyarrow
```

### Script Content (`generate_parquet_data.py`)

```python
import os
import pyarrow as pa
import pyarrow.parquet as pq

# Define base path for test data
BASE_PATH = "crates/connectors/iceberg/test_data"
ICEBERG_TABLE_B_DATA_PATH = os.path.join(BASE_PATH, "iceberg_table", "data")

# Ensure directories exist
os.makedirs(BASE_PATH, exist_ok=True)
os.makedirs(ICEBERG_TABLE_B_DATA_PATH, exist_ok=True)

# --- Create table_a (Parquet file) ---
schema_a = pa.schema([
    pa.field('id', pa.int64()),
    pa.field('data_a', pa.string())
])
data_a_rows = [
    (1, 'apple'),
    (2, 'banana'),
    (3, 'cherry'),
    (4, 'date'),
]
table_a_data = pa.Table.from_arrays([
    pa.array([row[0] for row in data_a_rows], type=pa.int64()),
    pa.array([row[1] for row in data_a_rows], type=pa.string())
], schema=schema_a)
parquet_file_a_path = os.path.join(BASE_PATH, "table_a.parquet")
pq.write_table(table_a_data, parquet_file_a_path)
print(f"Successfully wrote {parquet_file_a_path}")

# --- Create table_b_data_001.parquet (for Iceberg table_b) ---
schema_b_data = pa.schema([
    pa.field('id', pa.int64()),
    pa.field('data_b', pa.string())
])
data_b_rows = [
    (1, 'red'),    # Overlapping ID with table_a
    (3, 'green'),  # Overlapping ID with table_a
    (5, 'blue'),
    (6, 'yellow'),
    (7, 'purple')
]
table_b_data = pa.Table.from_arrays([
    pa.array([row[0] for row in data_b_rows], type=pa.int64()),
    pa.array([row[1] for row in data_b_rows], type=pa.string())
], schema=schema_b_data)

parquet_file_b_data_path = os.path.join(ICEBERG_TABLE_B_DATA_PATH, "table_b_data_001.parquet")
pq.write_table(table_b_data, parquet_file_b_data_path)
print(f"Successfully wrote {parquet_file_b_data_path}")

print("Parquet data generation complete.")
```

### Running the Script

To generate the Parquet files:
1.  Navigate to the repository root.
2.  Ensure `pyarrow` is installed in your Python environment.
3.  Run the script:
    ```bash
    # If pyigloo/.venv exists and is activated:
    # python generate_parquet_data.py
    # Or specify the python from the venv:
    pyigloo/.venv/bin/python generate_parquet_data.py
    ```
    The script should be placed in a location where it can be run, e.g., at the root of the repository or alongside this README. For this subtask, it was created at the root.

## Iceberg Table `table_b` Structure

The `table_b` Iceberg table is located in `crates/connectors/iceberg/test_data/iceberg_table/`. Its metadata is manually constructed as a simplified setup due to difficulties in programmatic generation with `pyiceberg==0.9.1`.

### Directory Structure

```
iceberg_table/
├── data/
│   └── table_b_data_001.parquet
└── metadata/
    ├── snap-placeholder.txt
    ├── v1.metadata.json
    └── version-hint.text
```

### Metadata Files

*   **`version-hint.text`**:
    *   Content: `1`
    *   Indicates that `v1.metadata.json` is the current metadata version.

*   **`v1.metadata.json`**:
    *   This file defines the schema, partitioning, sort order, snapshots, etc., for `table_b`.
    *   Key details:
        *   `format-version`: "1"
        *   `table-uuid`: A fixed UUID.
        *   `location`: Points to the root of the `iceberg_table` directory.
        *   `schema`: Defines `id` (long, required, field ID 1, identifier) and `data_b` (string, optional, field ID 2).
        *   `partition-spec`: Empty, indicating an unpartitioned table.
        *   `current-snapshot-id`: -1 (no current snapshot, as the manifest is a placeholder).
        *   `snapshots`: Empty list.
    *   The full content is:
        ```json
        {
          "format-version": "1",
          "table-uuid": "f7c3f5a5-3f28-4c22-94e8-0a8f2ad680f8",
          "location": "file:///app/crates/connectors/iceberg/test_data/iceberg_table",
          "last-updated-ms": 1678886400000,
          "last-column-id": 2,
          "schema": {
            "type": "struct",
            "schema-id": 0,
            "identifier-field-ids": [1],
            "fields": [
              {
                "id": 1,
                "name": "id",
                "required": true,
                "type": "long"
              },
              {
                "id": 2,
                "name": "data_b",
                "required": false,
                "type": "string"
              }
            ]
          },
          "schemas": [
            {
              "type": "struct",
              "schema-id": 0,
              "identifier-field-ids": [1],
              "fields": [
                {
                  "id": 1,
                  "name": "id",
                  "required": true,
                  "type": "long"
                },
                {
                  "id": 2,
                  "name": "data_b",
                  "required": false,
                  "type": "string"
                }
              ]
            }
          ],
          "current-schema-id": 0,
          "partition-spec": [],
          "default-spec-id": 0,
          "partition-specs": [
            {
              "spec-id": 0,
              "fields": []
            }
          ],
          "default-sort-order-id": 0,
          "sort-orders": [
            {
              "order-id": 0,
              "fields": []
            }
          ],
          "properties": {
            "owner": "test_user",
            "comment": "Manually created test Iceberg table_b"
          },
          "current-snapshot-id": -1,
          "snapshots": [],
          "snapshot-log": [],
          "metadata-log": []
        }
        ```

*   **`snap-placeholder.txt` (Manifest File Placeholder)**:
    *   This file is a **placeholder** for a real Avro-encoded Iceberg manifest file.
    *   A valid manifest would list `../data/table_b_data_001.parquet` and its statistics.
    *   This placeholder is used because programmatic generation of a valid manifest file with `pyiceberg==0.9.1` was unsuccessful in previous attempts.
    *   The Iceberg reader implementation in `iceberg-rust` should initially focus on parsing schema information from `v1.metadata.json`. Full table scan functionality will likely require a valid manifest file.
    *   Content:
        ```text
        This is a placeholder for a real Avro-encoded Iceberg manifest file.
        A valid manifest file (e.g., snap-xxxxxxxx-m00000.avro) needs to be generated that points to:
        ../data/table_b_data_001.parquet
        The manifest file should contain information about the data file, including its path, format (Parquet),
        partition data (none for this unpartitioned table), record count, file size, and column-level stats if available.
        This placeholder is used because programmatic generation of a valid manifest file with pyiceberg==0.9.1
        proved too difficult due to library import and API issues.
        The Iceberg reader implementation should initially focus on parsing schema information from vX.metadata.json
        and might not support full table scans until a valid manifest is provided.
        ```

This setup provides the basic file structure and metadata for `iceberg-rust` to begin implementing read support, focusing on schema parsing first.
The `generate_parquet_data.py` script should be co-located or easily accessible for regenerating the base Parquet files. For this subtask, it was created at the root of the repository. It might be better to move it to `crates/connectors/iceberg/test_data/`.
