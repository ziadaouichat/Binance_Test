from binance import Client
import pandas as pd
from statsmodels.tsa.seasonal import seasonal_decompose
import logging
import time
from Binance_Config import apiKey, secret
import datetime as dt

# Set up logging
logging.basicConfig(filename='live_trading_log.txt', level=logging.ERROR,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize Binance connection
client = Client(apiKey, secret)

# Symbol and timeframe settings
symbol = 'BTCUSDT'

limit = "20h"

# Function to fetch historical price data
def fetch_price_data(symbol, limit):
    try:
        klines = client.get_historical_klines(symbol, Client.KLINE_INTERVAL_1MINUTE, limit)
        df = pd.DataFrame(klines, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'qav', 'num_trades', 'taker_base_vol', 'taker_quote_vol', 'ignore'])

        # Change timestamp
        df.index = [dt.datetime.fromtimestamp(x / 1000.0) for x in df.close_time]
        df = df.astype(float)

        # Print the last 10 'close' values
        print(df['close'])
        return df
    except Exception as e:
        print(f"Error fetching data: {e}")
        return pd.DataFrame()

# Function to decompose price series
def decompose_price(price_data):
    try:
        result = seasonal_decompose(price_data['close'], model='multiplicative', period=9)
        residuals = result.resid.dropna()
        residuals = pd.DataFrame({'residuals': residuals})
        # Print the individual components of decomposition
        print("Trend Component:", result.trend)
        print("Seasonal Component:", result.seasonal)
        print("Residuals:", residuals)
        return residuals
    except Exception as e:
        logging.error(f"Failed to decompose price series. Error: {e}")
        return pd.DataFrame()

# Function to place a buy order
def BUY(symbol, volume):
    try:
        order = client.create_limit_buy_order(
            symbol=symbol,
            quantity=volume,
            price=client.fetch_ticker(symbol)['ask']
        )
        logging.info(f"Buy order placed for {volume} units of {symbol}. Order ID: {order['id']}")
    except Exception as e:
        logging.error(f"Failed to place Buy order. Error: {e}")

# Function to place a sell order
def SELL(symbol, volume):
    try:
        order = client.create_limit_sell_order(
            symbol=symbol,
            quantity=volume,
            price=client.fetch_ticker(symbol)['bid']
        )
        logging.info(f"Sell order placed for {volume} units of {symbol}. Order ID: {order['id']}")
    except Exception as e:
        logging.error(f"Failed to place Sell order. Error: {e}")

while True:
    # Fetch historical price data
    price_data = fetch_price_data(symbol, limit)

    if price_data.empty:
        logging.error("Failed to fetch price data. Retrying in 60 seconds.")
        time.sleep(60)
        continue

    # Keep only the most recent data
    price_data = price_data[-int(limit[:-1]):]

    residuals = decompose_price(price_data)

    if residuals.empty:
        logging.error("Failed to decompose price series. Retrying in 60 seconds.")
        time.sleep(60)
        continue

    try:
        # Calculate rolling mean and standard deviation
        rolling_mean = residuals['residuals'].rolling(window=9).mean()
        rolling_std = residuals['residuals'].rolling(window=9).std()

        # Calculate upper and lower thresholds dynamically
        residuals['upper_threshold'] = residuals['residuals'] + 2 * rolling_std
        residuals['lower_threshold'] = residuals['residuals'] - 2 * rolling_std

        # Reindex lower_threshold to match rolling_mean index
        residuals['lower_threshold'] = residuals['lower_threshold'].reindex(rolling_mean.index).fillna(method='ffill')

        # Identify the start of new buy and sell signals
        residuals['Sell_signal_start'] = (rolling_mean > residuals['lower_threshold']) & (
                    rolling_mean.shift(1) <= residuals['lower_threshold'].shift(1))
        residuals['Buy_signal_start']  = (rolling_mean < residuals['upper_threshold']) & (
                    rolling_mean.shift(1) >= residuals['upper_threshold'].shift(1))

        # Define buy and sell signals
        residuals['Sell_signal'] = residuals['Sell_signal_start'].astype(int)
        residuals['Buy_signal'] = residuals['Buy_signal_start'].astype(int)

        # Remove repeated signals
        residuals['Sell_signal'] = residuals['Sell_signal'] - residuals['Sell_signal'].shift(1)
        residuals['Buy_signal'] = residuals['Buy_signal'] - residuals['Buy_signal'].shift(1)

        # Replace signal values with 'Buy' or 'Sell'
        residuals['Sell_signal'] = residuals['Sell_signal'].apply(lambda x: 'Sell' if x == 1 else '')
        residuals['Buy_signal'] = residuals['Buy_signal'].apply(lambda x: 'Buy' if x == 1 else '')

        # Create 'Signals' column
        residuals['Signals'] = residuals['Sell_signal'] + residuals['Buy_signal']

        # Print the DataFrame
        print(residuals["Signals"].tail(50))
        #print("Rolling Mean:", rolling_mean)
        #print("Rolling Std:", rolling_std)

        # Count the number of buy and sell signals
        buy_signal_count = residuals['Buy_signal_start'].sum()
        sell_signal_count = residuals['Sell_signal_start'].sum()

        # Print the signal counts
        print(f'Buy signals count: {buy_signal_count}')
        print(f'Sell signals count: {sell_signal_count}')
        
        #print("Residuals:", residuals)

    except Exception as e:
        logging.error(f"An error occurred during analysis. Error: {e}")

    # Wait for the next iteration
    print("******************************************")
    time.sleep(60)  # Adjust the sleep duration as needed
    
    
    
