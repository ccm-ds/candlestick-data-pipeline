import os
import dask.dataframe as dd
from pathlib import Path


def read_data_by_file_extension(file_path: Path) -> dd:
    """
    read data file into dask data frame based on the input path file extension
    :param file_path: path of input data
    :return: dask dataframe
    """
    map_file_extension_to_read_function = {'.csv': dd.read_csv, '.parquet': dd.read_parquet, '.json': dd.read_json}
    name, extension = os.path.splitext(file_path)
    if extension.lower() in map_file_extension_to_read_function.keys():
        read_function = map_file_extension_to_read_function[extension.lower()]
        return read_function(file_path)
    else:
        raise Exception(f"File extention {extension} not recognized")


def write_data_by_file_extension(data: dd = None, file_path: Path = None):
    """
    write dask dataframe to file into based on the input path file extension
    :param file_path: path of output file
    :return: None
    """
    data = data.compute()
    map_file_extension_to_read_function = {'.csv': 'to_csv', '.parquet': 'to_parquet'}
    name, extension = os.path.splitext(file_path)
    if extension.lower() in map_file_extension_to_read_function.keys():
        write_function = getattr(data, map_file_extension_to_read_function[extension.lower()])
        read_function = map_file_extension_to_read_function[extension.lower()]
        write_function(file_path, index=False)
    else:
        raise Exception(f"File extention {extension} not recognized")
