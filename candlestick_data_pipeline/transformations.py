import dask.dataframe as dd
import pandas as pd
from typing import List, Tuple


def format_date_columns(data: dd = None, date_columns: List[str] = None) -> dd:
    """
    Format all columns specified in data_columns to ISO8601 date fromat
    :param data: dask dataframe
    :param date_columns: name of date columns
    :return: dask dataframe
    """
    for date_column in date_columns:
        data[date_column] = dd.to_datetime(dd.to_datetime(data[date_column]), format='%Y-%m-%d')
    return data


def drop_rows_with_any_null_values(data: dd = None) -> dd:
    """
    drop and rows containing null values from the input dataframe
    :param data: dask dataframe
    :return: modified dask dataframe
    """
    return data.dropna()


def drop_duplicate_rows(data: dd = None, subset: List[str] = None, keep: str = None) -> dd:
    """
    Drop rows containing duplicate data for the specified subset of columns
    :param data: dask dataframe
    :param subset: list of column names
    :param keep: which duplicate to keep
    :return: modified dask dataframe
    """
    return data.drop_duplicates(subset=subset, keep=keep)


def filter_by_date_range(data: dd = None, date_range: Tuple[str] = None, date_column: str = None) -> dd:
    """
    filter out rows of dataframe containing dates outside of specified date range
    :param data: dask dataframe
    :param date_range: tuple of min and max date
    :param date_column: name of date column
    :return: modified dask dataframe
    """
    data = format_date_columns(data=data, date_columns=[date_column])
    data = data.loc[(data[date_column] >= date_range[0]) & (data[date_column] <= date_range[1])]
    return data


def agg_insert_by_group(data: dd = None, groupby_columns: List[str] = None, agg_dict: dict = None,
                        insert_dict: dict = None) -> dd:
    """
    Split input dataframe into groups, apply aggregations on each group according to the aggregation dict, 
    insert aggregated results back into the original dataframe with column values specified in insert dict
    :param data: input dask dataframe
    :param groupby_columns: list of column names to group by
    :param agg_dict: dictionary of the format {column name: aggregation to preform to column name}
    :param insert_dict: dictionary of the format {column name: value of column to be set prior to insertion}
    :return: modified datafraeme
    """
    agg_data = data.groupby(groupby_columns).agg(agg_dict).reset_index()
    agg_data.columns = agg_data.columns.droplevel(1)
    for column, value in insert_dict.items():
        agg_data[column] = 'COMBINED'
    data = data.append(agg_data)
    return data


def rolling_mean_by_date_by_group(data: dd = None, groupby_columns: List[str] = None, metric_columns: List[str] = None,
                                  date_column: str = None, window: int = None) -> dd:
    """
    Split input dateframe into groups and preform a rolling average on the metric columns for each group
    :param data: input dataframe
    :param groupby_columns: list of columns to group by
    :param metric_columns: columns to calculate rolling average on
    :param date_column: name of date column
    :param window: window size to be used on rolling average
    :return: modified dask dataframe
    """
    data = data.set_index(date_column, sorted=True)
    output_schema = dict(data.dtypes)
    for metric_column in metric_columns:
        output_schema[f'{metric_column}_rolling'] = 'float32'
    output_schema = list(output_schema.items())
    data = data.groupby(by=groupby_columns).apply(
        lambda df_g: rolling_mean_by_date(data=df_g, metric_columns=metric_columns, window=window), meta=output_schema)
    data = data.reset_index().rename(columns={'index': date_column})
    return data


def rolling_mean_by_date(data: dd = None, metric_columns: List[str] = None, window: int = None) -> dd:
    """
    preform rolling average on a single group
    :param data: 
    :param metric_columns: 
    :param window: 
    :return: 
    """
    for metric_column in metric_columns:
        output_column_name = f'{metric_column}_rolling'
        data[output_column_name] = data[metric_column].rolling(window=window, min_periods=0).mean().astype('float32')
    return data


def yoy_percent_change_by_group(data: dd = None, groupby_columns: List[str] = None, metric_columns: List[str] = None,
                                date_column: str = None) -> dd:
    """
    Split dataframe into groups and calculate year over year percent change for the etric columns in each group
    :param data: input dataframe
    :param groupby_columns: list of columns to group by
    :param metric_columns: columns to calculate rolling average on
    :param date_column: name of date column
    :return: modified dataframe
    """
    data = data.set_index(date_column, sorted=True)
    output_schema = dict(data.dtypes)
    for metric_column in metric_columns:
        output_schema[f'{metric_column}_yoy_pct_change'] = 'float32'
    output_schema = list(output_schema.items())
    data = data.groupby(by=groupby_columns).apply(
        lambda df_g: yoy_percent_change(data=df_g, metric_columns=metric_columns), meta=output_schema)
    data = data.reset_index().rename(columns={'index': date_column})
    return data


def yoy_percent_change(data: dd = None, metric_columns: List[str] = None) -> dd:
    """
    calculate yoy pct change for a single group
    """
    for metric_column in metric_columns:
        output_column_name = f'{metric_column}_yoy_pct_change'
        data[output_column_name] = data[metric_column].pct_change(365).astype('float32')
    return data


def fill_missing_dates_by_group(data: dd = None, groupby_columns: List[str] = None, fill_method=None,
                                date_range: Tuple[str] = None, date_column: str = None) -> dd:
    """
    split input dataframe into groups according to groupby columns and reindex with continuous dates with specified 
    date range. Fill missing values according to fill method
    :param data: dataframe
    :param groupby_columns: list of columns to groupby 
    :param fill_method: method used to fill missing data
    :param date_range: date range to reidex to
    :param date_column: name of date column
    :return: modified dataframe
    """
    output_schema = dict(data.dtypes)
    output_schema = list(output_schema.items())
    data = data.groupby(by=groupby_columns).apply(
        lambda df_g: fill_missing_dates(data=df_g, date_column=date_column, fill_method=fill_method,
                                        date_range=date_range),
        meta=output_schema).reset_index(drop=True)
    return data


def fill_missing_dates(data: dd = None, date_column: str = None, fill_method: str = None,
                       date_range: Tuple[str] = None) -> dd:
    """
    Preform date fill on single group
    """
    # all_dates = data.reset_index().set_index(date_column).resample('D').asfreq().index
    all_dates = pd.date_range(date_range[0], date_range[1])
    data = data.set_index(date_column).reindex(all_dates, method=fill_method)
    data = data.reset_index().rename(columns={'index': date_column})
    return data
