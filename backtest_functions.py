import pandas as pd
import numpy as np


# Global variable to keep track of the position
position = 0

def update_position(row, price_col, SMA_col, EMA_col):
    """
    This function is created to be passed in an apply method on a Pandas Dataframe.
    The goal is to update the position of our portfolio based on some defined signals and avoid a loop.
    """
    global position
    close = row[price_col]
    sma = row[SMA_col]
    ema = row[EMA_col]

    if position == 0:
        if close > sma and close > ema: #if close price above both SMA and EMA, buy (if we didn't hold the crypto before)
            position = 1
    elif position == 1: #if close price below either SMA or EMA, sell (if we held the crypto before)
        if close <= sma or close <= ema:
            position = 0

    return position

def trend_following(df, price_col, strategy_col, window=200):
    """
    df is a Pandas DataFrame of historical prices.
    price_col is the name of the price column.
    """
    global position  # Reset the global position variable for each call to trend_following
    position = 0
    
    df = df.copy()
    
    SMA_col = f'SMA_{window}'
    EMA_col = f'EMA_{window}'
    df[SMA_col] = df[price_col].rolling(window).mean()
    df[EMA_col] = df[price_col].ewm(span=window).mean()

    # Use apply along with the update_position function defined above to update the position column
    df['position'] = df.apply(update_position, args=(price_col, SMA_col, EMA_col), axis=1).shift(1) #This shift is very important - we only update our position one day after the signal has been detected. So in that way we are avoiding look-ahead bias.
    
    df[strategy_col] = df[price_col].pct_change() * df['position']
    
    return df

def max_drawdown(df, strategy_col):
    #df['return'] = df[price_col].pct_change()
    df[f'cumulative_return_{strategy_col}'] = (1+df[f'{strategy_col}']).cumprod() - 1
    df['drawdown'] = 1 - (1 + df[f'cumulative_return_{strategy_col}']) / (1 + df[f'cumulative_return_{strategy_col}'].cummax())
    max_drawdown = df['drawdown'].max()
    return max_drawdown


def return_metrics(df, price_col, strategy_col, frequency='daily'):
    
    """
    This function takes as arguments a pandas dataframe with the daily returns of a certain strategy and returns the Total 
    Performance, the CAGR, the annualized volatility, the Sharpe Ratio and the Max Drawdown of the strategy.
    
    """
    cum_return_col = f'cumulative_{strategy_col}'
    
    
    df[cum_return_col] = (1 + df[strategy_col]).cumprod() - 1
    T = len(df)
    #We calculate the parameter h which is used to calculate the CAGR of the strategy, based on the frequency of the returns data.
    if frequency == 'daily':
        h = 365 #cryptos are traded everyday of the year.
    elif frequency == 'hourly':
        h = 365*24
    
    elif frequency == '5-minute':
        h = 60/5*365*24
    
    n_years = T/h
    total_performance = df[cum_return_col].iloc[-1]
    v_initial = 100
    v_final = 100 * (1+df[cum_return_col].iloc[-1])
    annualized_return = (v_final / v_initial) ** (1/n_years) - 1

    annualized_vol = df[price_col].std() * np.sqrt(h)
    sharpe_ratio = annualized_return / annualized_vol
    
    max_dd = max_drawdown(df, strategy_col)
    
    #df['drawdown'] = 1 - (1 + df[cum_return_col]) / (1 + df[cum_return_col].cummax())
    #max_drawdown = df['drawdown'].max()
    
    return total_performance, annualized_return, annualized_vol, sharpe_ratio, max_dd
    


def grid_search(data, price_col, strategy_col, window_range=range(10, 201, 10)):
    """
    This function applies a brut force grid search to search for the optimal window of a trend following strategy.
    The goal is to maximize risk-adjusted return as defined by the ratio of Sharpe Ratio divided by Max Drawdown.
    So we are trying to have the largest possible return with lowest possible volatility and max drawdown.
    We could also decide to optimize for another parameter.
    """
    optimal_window = None
    max_objective_value = -np.inf  # Initialize to negative infinity
    results = []

    for window_size in window_range:  # Loop from 10 to 200 in steps of 10
        df = trend_following(data, price_col, strategy_col, window=window_size)
        _, _, _, sharpe_ratio, max_drawdown = return_metrics(df, price_col, strategy_col)
        objective_value = sharpe_ratio / max_drawdown
        results.append([window_size, objective_value])

        if objective_value > max_objective_value:
            max_objective_value = objective_value
            optimal_window = window_size

    return optimal_window