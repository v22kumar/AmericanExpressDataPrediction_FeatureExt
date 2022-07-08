import dask.dataframe as dd  #import dask python lib#
from features import train_parquet

def count_missing(ddf):
    ncount = 0
    for c in ddf.columns:
        ncount = ncount + len(train_parquet[c]) - train_parquet[c].count().compute()
    return ncount

def fill_missing(ddf):
    for c in ddf.columns:
        if ddf[c].dtype == 'float64':
            ddf[c] = ddf[c].astype('float64').fillna(0.0)
    return ddf

