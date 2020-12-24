import pandas as pd

def read_data_from_csv(filepath):
  df = pd.read_csv(filepath)
  return df
