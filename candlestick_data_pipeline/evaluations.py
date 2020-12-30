import dask.dataframe as dd
import pandas as pd
from functools import partial


def date_continuity_check_by_group(data=None, groupby_columns=None, date_column=None):
    output_schema = [(date_column, data[date_column].dtype)]
    output_schema.append(('date_continuity_bool', 'bool'))
    data = data.groupby(by=groupby_columns).apply(
        lambda df_g: date_continuity_check(data=df_g, date_column=date_column),
        meta=output_schema).reset_index()
    return data['date_continuity_bool'].compute().any()


def date_continuity_check(data=None, date_column=None):
    day = pd.Timedelta('1d')
    data['date_continuity_bool'] = ((data[date_column] - data[date_column].shift(-1)).abs() > day) | \
                                   (data[date_column].diff() > day)
    return data[[date_column, 'date_continuity_bool']]


def null_data_check(data=None):
    data = data.compute()
    return data.isnull().values.any()


def date_range_check_by_group(data=None, groupby_columns=None, date_range=None, date_column=None):
    output_schema = ('date_range_bool', 'bool')
    data = data.groupby(by=groupby_columns).apply(
        lambda df_g: date_range_check(data=df_g, date_range=date_range, date_column=date_column),
        meta=output_schema).reset_index()
    return data['date_range_bool'].compute().any()


def date_range_check(data=None, date_range=None, date_column=None):
    f = partial(pd.to_datetime)
    data['date_range_bool'] = (data[date_column].min() > f(date_range[0])) | (data[date_column].max() < f(date_range[1]))
    return data['date_range_bool']
