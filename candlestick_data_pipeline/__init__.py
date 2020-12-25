import dask.dataframe as dd
import os

def read_data_by_file_extension(file_path):
  """
  parse file path
  :param file_path: path of
  :return:
  """
  map_file_extension_to_read_function = {'.csv': dd.read_csv, '.parquet': dd.read_parquet, '.json': dd.read_json}
  name, extension = os.path.splitext(file_path)
  if extension.lower() in file_extension_to_read_funtion_lookup.keys():
    read_function = file_extension_to_read_funtion_lookup[extension.lower()]
    return read_function(file_path)
  else:
    raise Exception(f"File extention {extension} not recognized")

