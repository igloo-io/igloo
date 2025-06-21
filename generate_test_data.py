import os
import uuid
import time
import pyarrow as pa
import pyarrow.parquet as pq

# Attempting to find the correct locations for PyIceberg 0.9.1 classes
# These imports are based on the previous debugging iterations and common PyIceberg structure.
# Adjustments might be needed if errors persist.
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, StringType, NestedField
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.table.sorting import SortOrder
# `Snapshot` is often in `pyiceberg.table` or `pyiceberg.table.snapshots`
from pyiceberg.table import Snapshot
# `FileFormat` for DataFiles is often in `pyiceberg.manifest` or `pyiceberg.types`
from pyiceberg.manifest import DataFile, FileFormat, ManifestContent
# `AvroManifestWriter` could be in `pyiceberg.io.pyarrow` or `pyiceberg.avro`
# For 0.9.1, it's likely in `pyiceberg.io.pyarrow` if it uses PyArrow for writing.
# Let's try a common location first.
from pyiceberg.io.pyarrow import PyArrowFileIO # Corrected import path
from pyiceberg.io import OutputFile # OutputFile might be directly under pyiceberg.io
# The writer itself might be specific to an IO implementation (like PyArrow)
# Trying to locate AvroManifestWriter, e.g. from pyiceberg.io.pyarrow or pyiceberg.avro.
# Based on common patterns, if PyArrowFileIO is used, the writer might be related.
# A common pattern is pyiceberg.avro.AvroManifestWriter if it's a generic Avro writer.
# Or pyiceberg.io.pyarrow.PyArrowManifestWriter.
# Let's assume a generic Avro writer for now if a PyArrow specific one isn't found.
# The previous report indicated `AvroManifestWriter` was the target.
# pyiceberg 0.9.1 `pyiceberg/avro/reader_writer.py` defines `AvroManifestWriter`.
# Trying to import from pyiceberg.io.pyarrow as it's related to PyArrowFileIO
from pyiceberg.io.pyarrow import AvroManifestWriter

from pyiceberg.table.metadata import TableMetadataV2

# Define base path for test data
BASE_PATH = "crates/connectors/iceberg/test_data"
ICEBERG_TABLE_PATH = os.path.join(BASE_PATH, "iceberg_table")
ICEBERG_DATA_PATH = os.path.join(ICEBERG_TABLE_PATH, "data")
ICEBERG_METADATA_PATH = os.path.join(ICEBERG_TABLE_PATH, "metadata")

# Ensure directories exist
os.makedirs(BASE_PATH, exist_ok=True)
os.makedirs(ICEBERG_DATA_PATH, exist_ok=True)
os.makedirs(ICEBERG_METADATA_PATH, exist_ok=True)

print(f"Target base path: {os.path.abspath(BASE_PATH)}")
print(f"Iceberg table path: {os.path.abspath(ICEBERG_TABLE_PATH)}")

# --- Create table_a (Parquet file) ---
schema_a = pa.schema([
    pa.field('id', pa.int64()),
    pa.field('data_a', pa.string())
])
data_a_rows = [(1, 'apple'), (2, 'banana'), (3, 'cherry'), (4, 'date')]
table_a_data = pa.Table.from_arrays([
    pa.array([row[0] for row in data_a_rows], type=pa.int64()),
    pa.array([row[1] for row in data_a_rows], type=pa.string())
], schema=schema_a)
parquet_file_a_path = os.path.join(BASE_PATH, "table_a.parquet")
pq.write_table(table_a_data, parquet_file_a_path)
print(f"Successfully wrote {parquet_file_a_path}")

# --- Create table_b (Iceberg table) ---
# Schema definition using NestedField and model_validate for robustness
id_field_data = {"id": 1, "name": "id", "type": IntegerType(), "required": True}
id_field = NestedField.model_validate(id_field_data)
data_b_field_data = {"id": 2, "name": "data_b", "type": StringType(), "required": True}
data_b_field = NestedField.model_validate(data_b_field_data)
iceberg_schema_b = Schema(
    fields=(id_field, data_b_field),
    schema_id=1,
    identifier_field_ids=[1]
)

# Arrow schema for writing Parquet data for table_b
arrow_schema_b_for_parquet = pa.schema([
    pa.field('id', pa.int64()),
    pa.field('data_b', pa.string())
])
data_b_rows = [(1, 'red'), (3, 'green'), (5, 'blue'), (6, 'yellow')]
table_b_data_for_parquet = pa.Table.from_arrays([
    pa.array([row[0] for row in data_b_rows], type=pa.int64()),
    pa.array([row[1] for row in data_b_rows], type=pa.string())
], schema=arrow_schema_b_for_parquet)

parquet_file_b_name = "part-00000-example.parquet"
parquet_file_b_path = os.path.join(ICEBERG_DATA_PATH, parquet_file_b_name)
pq.write_table(table_b_data_for_parquet, parquet_file_b_path)
print(f"Successfully wrote {parquet_file_b_path}")

# Iceberg table properties
table_properties = {'owner': 'test_user', 'comment': 'Test Iceberg table b'}
location = f"file://{os.path.abspath(ICEBERG_TABLE_PATH)}"

# Partitioning (unpartitioned)
spec = PartitionSpec(schema=iceberg_schema_b, spec_id=0)

# Sorting (unsorted)
sort_order = SortOrder(schema=iceberg_schema_b, order_id=0)

# DataFile entry
data_file_relative_path = os.path.join("data", parquet_file_b_name)
file_system_file_size = os.path.getsize(parquet_file_b_path)

# For unpartitioned tables, partition data is an empty tuple or specific Row object
# Using empty tuple as per previous debugging outcome.
partition_values = ()

manifest_entry = DataFile(
    content=ManifestContent.DATA, # Explicitly use ManifestContent.DATA
    file_path=data_file_relative_path,
    file_format=FileFormat.PARQUET, # Use FileFormat enum
    partition=partition_values,
    record_count=len(data_b_rows),
    file_size_in_bytes=file_system_file_size,
    sort_order_id=sort_order.order_id
)

# Generate snapshot_id first
snapshot_id = int(time.time() * 1000)

# Create Manifest File using AvroManifestWriter
manifest_file_uuid = uuid.uuid4()
manifest_path_relative = os.path.join("metadata", f"snap-{snapshot_id}-{manifest_file_uuid}-m0.avro")
manifest_path_full = os.path.join(ICEBERG_TABLE_PATH, manifest_path_relative)

file_io = PyArrowFileIO(properties={})
output_file_obj = file_io.new_output(manifest_path_full)

# AvroManifestWriter(self, output_file: OutputFile, snapshot_id: int, schema: Schema, spec: PartitionSpec, content: ManifestContent = ManifestContent.DATA)
with AvroManifestWriter(output_file_obj, snapshot_id, iceberg_schema_b, spec, content=ManifestContent.DATA) as writer:
    writer.add(manifest_entry) # add() is the method for AvroManifestWriter to add DataFile
print(f"Successfully wrote Iceberg manifest file: {manifest_path_full}")

# Snapshot
snapshot = Snapshot(
    snapshot_id=snapshot_id,
    parent_snapshot_id=None,
    sequence_number=0,
    timestamp_ms=int(time.time() * 1000000), # snapshot ts in micros
    manifest_list=manifest_path_relative,
    summary={"operation": "append"}, # string literal for operation
    schema_id=iceberg_schema_b.schema_id
)

# Table Metadata
table_uuid = uuid.uuid4()
table_metadata = TableMetadataV2(
    table_uuid=table_uuid,
    location=location,
    last_sequence_number=0, # Assuming this is the first version
    last_updated_ms=int(time.time() * 1000000), # metadata update ts in micros
    last_column_id=iceberg_schema_b.highest_field_id,
    schemas=[iceberg_schema_b],
    current_schema_id=iceberg_schema_b.schema_id,
    partition_specs=[spec],
    default_spec_id=spec.spec_id,
    sort_orders=[sort_order],
    default_sort_order_id=sort_order.order_id,
    properties=table_properties,
    current_snapshot_id=snapshot.snapshot_id,
    snapshots=[snapshot],
    snapshot_log=[],
    metadata_log=[],
    format_version=2,
)

metadata_json_filename = f"v1.metadata.json" # Version 1 of the metadata file
metadata_file_path = os.path.join(ICEBERG_METADATA_PATH, metadata_json_filename)
with open(metadata_file_path, "w") as f:
    f.write(table_metadata.model_dump_json(indent=2))
print(f"Successfully wrote Iceberg metadata: {metadata_file_path}")

version_hint_file_path = os.path.join(ICEBERG_METADATA_PATH, "version-hint.text")
with open(version_hint_file_path, "w") as f:
    f.write("1\n") # Hint points to v1.metadata.json
print(f"Successfully wrote Iceberg version hint: {version_hint_file_path}")

print("Test data generation complete.")

# Cleanup (optional, for example if run in a test setup)
# print(f"To cleanup, remove: {os.path.abspath(BASE_PATH)}")
# import shutil
# shutil.rmtree(BASE_PATH)
# print("Cleanup complete.")
