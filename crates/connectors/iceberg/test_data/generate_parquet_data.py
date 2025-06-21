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
