import matplotlib.pyplot as plt

# OUTPUT_DATA_PATH = f'{os.getcwd()}/output.csv'
def example_visualization(data=None):

    SYMBOLS = ['FIVE','CMG','BURL']
    for symbol in SYMBOLS:
        df = data.loc[data.symbol==symbol]

        fig, axes = plt.subplots(nrows=3, ncols=1)
        fig.tight_layout()

        plot_df = df.pivot(index='date', columns='cardtype', values='metric')
        plot_df.plot(ax=axes[0],title=f'All Card Types - Symbol: {symbol}')

        plot_df= df.loc[df.cardtype=='COMBINED'][['date','metric','metric_rolling_sum']]
        plot_df.plot(x='date',ax=axes[1], title=f'COMBINED metric_rolling - Symbol: {symbol}')

        plot_df= df.loc[df.cardtype=='COMBINED'][['date','metric_rolling_sum_yoy_pct_change']]
        plot_df.plot(x='date',ax=axes[2], title=f'COMBINED metric_rolling YoY %delta - Symbol: {symbol}')

    plt.show()