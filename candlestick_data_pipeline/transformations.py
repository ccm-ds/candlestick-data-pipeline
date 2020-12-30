import dask.dataframe as dd
import pandas as pd


def format_date_columns(data=None, date_columns=None):
    for date_column in date_columns:
        data[date_column] = dd.to_datetime(dd.to_datetime(data[date_column]), format='%Y-%m-%d')
    return data


def drop_rows_with_any_null_values(data=None):
    return data.dropna()


def drop_duplicate_rows(data=None, subset=None, keep=None):
    return data.drop_duplicates(subset=subset, keep=keep)


def filter_by_date_range(data=None, date_range=None, date_column=None):
    data = format_date_columns(data=data, date_columns=[date_column])
    data = data.loc[(data[date_column] >= date_range[0]) & (data[date_column] <= date_range[1])]
    return data


def agg_insert_by_group(data=None, groupby_columns=None, agg_dict=None, insert_dict=None):
    agg_data = data.groupby(groupby_columns).agg(agg_dict).reset_index()
    agg_data.columns = agg_data.columns.droplevel(1)
    for column, value in insert_dict.items():
        agg_data[column] = 'COMBINED'
    data = data.append(agg_data)
    return data


def rolling_mean_by_date_by_group(data=None, groupby_columns=None, metric_columns=None, date_column=None, window=None):
    data = data.set_index(date_column, sorted=True)
    output_schema = dict(data.dtypes)
    for metric_column in metric_columns:
        output_schema[f'{metric_column}_rolling'] = 'float32'
    output_schema = list(output_schema.items())
    data = data.groupby(by=groupby_columns).apply(
        lambda df_g: rolling_mean_by_date(data=df_g, metric_columns=metric_columns, window=window), meta=output_schema)
    data = data.reset_index().rename(columns={'index': date_column})
    return data


def rolling_mean_by_date(data=None, metric_columns=None, window=None):
    for metric_column in metric_columns:
        output_column_name = f'{metric_column}_rolling'
        data[output_column_name] = data[metric_column].rolling(window=window, min_periods=0).mean().astype('float32')
    return data


def yoy_percent_change_by_group(data=None, groupby_columns=None, metric_columns=None, date_column=None):
    data = data.set_index(date_column, sorted=True)
    output_schema = dict(data.dtypes)
    for metric_column in metric_columns:
        output_schema[f'{metric_column}_yoy_pct_change'] = 'float32'
    output_schema = list(output_schema.items())
    data = data.groupby(by=groupby_columns).apply(
        lambda df_g: yoy_percent_change(data=df_g, metric_columns=metric_columns), meta=output_schema)
    data = data.reset_index().rename(columns={'index': date_column})
    return data


def yoy_percent_change(data=None, metric_columns=None):
    for metric_column in metric_columns:
        output_column_name = f'{metric_column}_yoy_pct_change'
        data[output_column_name] = data[metric_column].pct_change(365).astype('float32')
    return data


def fill_missing_dates_by_group(data=None, groupby_columns=None, fill_method=None, date_range=None, date_column=None):
    output_schema = dict(data.dtypes)
    output_schema = list(output_schema.items())
    data = data.groupby(by=groupby_columns).apply(
        lambda df_g: fill_missing_dates(data=df_g, date_column=date_column, fill_method=fill_method, date_range=date_range),
        meta=output_schema).reset_index(drop=True)
    return data


def fill_missing_dates(data=None, date_column=None, fill_method=None, date_range=None):
    # all_dates = data.reset_index().set_index(date_column).resample('D').asfreq().index
    all_dates = pd.date_range(date_range[0], date_range[1])
    data = data.set_index(date_column).reindex(all_dates, method=fill_method)
    data = data.reset_index().rename(columns={'index': date_column})
    return data
