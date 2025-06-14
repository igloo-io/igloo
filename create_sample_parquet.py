import pyarrow as pa
import pyarrow.parquet as pq
import os

# Define the schema
schema = pa.schema([
    ('id', pa.int64()),
    ('name', pa.string()),
    ('value', pa.float64()),
    ('is_active', pa.bool_())
])

# Create some sample data
data = [
    pa.array([1, 2, 3, 4]),
    pa.array(['Alice', 'Bob', 'Charlie', 'David']),
    pa.array([10.5, 22.1, 15.0, 8.75]),
    pa.array([True, False, True, False])
]

# Create a RecordBatch
batch = pa.RecordBatch.from_arrays(data, schema=schema)

# Define the output path
output_dir = "crates/connectors/filesystem"
output_filename = "test_data.parquet"
output_path = os.path.join(output_dir, output_filename)

# Create the directory if it doesn't exist
os.makedirs(output_dir, exist_ok=True)

# Write the Parquet file
pq.write_table(pa.Table.from_batches([batch]), output_path)

print(f"Successfully created '{output_path}'")
