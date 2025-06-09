import pandas as pd
df = pd.DataFrame({'id': [1, 2, 3], 'value': ['foo', 'bar', 'baz']})
df.to_parquet('test_data.parquet')
