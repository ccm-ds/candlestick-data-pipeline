import dask.dataframe as dd

def read_data_from_csv(filepath):
  df = dd.read_csv(filepath)
  return df
