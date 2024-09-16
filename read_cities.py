import pandas as pd

cities = pd.read_parquet('data\\measurements_summary.parquet')

export = pd.DataFrame([cities['station'].unique()])

export.to_parquet("data/cities.parquet")

print(export)