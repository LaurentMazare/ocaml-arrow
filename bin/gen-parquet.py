import numpy as np
import pandas as pd

num_rows = 10**6
df = pd.DataFrame({
  'x': range(num_rows),
  'foo': ['foo'] * num_rows,
  'bar': [f'barbar{d}' for d in range(num_rows)],
})

for period in range(2, 21):
  df[f'baz{period}'] = [f'{d % period}-baz-periodic' for d in range(num_rows)]

df['x2'] = df['x'] * df['x']
df['cos_x'] = np.cos(df['x'])
df['sin_x'] = np.sin(df['x'])

for c in range(50):
  df[f'cos_x{c}'] = df['cos_x'] + c

df.to_parquet('foo.parquet')
