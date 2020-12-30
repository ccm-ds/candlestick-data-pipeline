import dask.dataframe as dd
import pandas as pd
from functools import partial
from typing import List, Tuple


def date_continuity_check_by_group(data: dd = None, groupby_columns: List[str] = None, date_column: str = None) -> bool:
    """
    Split data into groups and evaluate each group checking if it contains a set of continuous dates in its date column.
    If any group contains a discontinuity return true else return false
    :param data: dask dataframe
    :param groupby_columns: column names to groupby
    :param date_column: date column name
    :return: boolean
    """
    output_schema = [(date_column, data[date_column].dtype)]
    output_schema.append(('date_continuity_bool', 'bool'))
    data = data.groupby(by=groupby_columns).apply(
        lambda df_g: date_continuity_check(data=df_g, date_column=date_column),
        meta=output_schema).reset_index()
    return data['date_continuity_bool'].compute().any()


def date_continuity_check(data: dd = None, date_column: str = None) -> dd:
    """
    preform date continuity check for a single group
    """
    day = pd.Timedelta('1d')
    data['date_continuity_bool'] = ((data[date_column] - data[date_column].shift(-1)).abs() > day) | \
                                   (data[date_column].diff() > day)
    return data[[date_column, 'date_continuity_bool']]


def null_data_check(data: dd = None) -> bool:
    """
    Check if dataframe contains any null values if so return true
    """
    data = data.compute()
    return data.isnull().values.any()


def date_range_check_by_group(data: dd = None, groupby_columns: List[str] = None, date_range: Tuple[str] = None,
                              date_column: str = None) -> bool:
    """
    Split input dataframe by group and check if the min and max date of each group falls outside of specified date range
    :param data: dask dataframe
    :param groupby_columns: list of column names to group by
    :param date_range: tuple defining required date range
    :param date_column: name of date column
    :return: bool
    """
    output_schema = ('date_range_bool', 'bool')
    data = data.groupby(by=groupby_columns).apply(
        lambda df_g: date_range_check(data=df_g, date_range=date_range, date_column=date_column),
        meta=output_schema).reset_index()
    return data['date_range_bool'].compute().any()


def date_range_check(data: dd = None, date_range: Tuple[str] = None, date_column: str = None) -> dd:
    """
    apply date range check on a single group
    """
    f = partial(pd.to_datetime)
    data['date_range_bool'] = (data[date_column].min() > f(date_range[0])) | (
                data[date_column].max() < f(date_range[1]))
    return data['date_range_bool']
