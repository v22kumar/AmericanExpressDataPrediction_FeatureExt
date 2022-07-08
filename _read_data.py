import pandas as pd

# Read CSV
train_data_path = '../input/amex-default-prediction/train_data.csv'

train_data = pd.read_csv(train_data_path, storage_options={'anon': True}, assume_missing=True)

# Parquet

name_function = lambda x: f"data-{x}.parquet"

train_data.to_parquet('./train_parquet/', name_function=name_function)

# Read Parquet Data

train_parquet_path = './train_parquet/data-0.parquet'

train_parquet = pd.read_parquet(train_parquet_path)

train_parquet.info()
