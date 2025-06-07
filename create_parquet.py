import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Create a Pandas DataFrame
data = {'col1': [1, 2, 3], 'col2': ['A', 'B', 'C']}
df = pd.DataFrame(data)

# Convert Pandas DataFrame to Arrow Table
table = pa.Table.from_pandas(df)

# Write Arrow Table to Parquet file
pq.write_table(table, 'crates/connectors/filesystem/test_data/sample.parquet')
print("Created crates/connectors/filesystem/test_data/sample.parquet")
