import config
import json
import math
import pandas as pd
import talib as ta

from binance import ThreadedWebsocketManager
from binance.client import Client
from chump import Application # Pushover
from csv import writer
from datetime import datetime
from functools import partial # Handle callback params

INTERVAL = Client.KLINE_INTERVAL_15MINUTE

SYMBOL = 'BTCUSDT'
PRECISION = 6 # https://www.binance.com/en/trade-rule

FAST_MA_TIMEPERIOD = 6
SLOW_MA_TIMEPERIOD = 35

SL_THRESHOLD = 0.995 # Stop-loss (0.5%)
TP_THRESHOLD = 1.051 # Take-profit (5.1%)

def get_data(client):
    klines = client.get_klines(symbol=SYMBOL, interval=INTERVAL)
    df = pd.DataFrame.from_records(klines)

    return df

def set_dema(df):
    df['FAST_MA'] = ta.DEMA(df['CLOSE'], timeperiod=FAST_MA_TIMEPERIOD)
    df['SLOW_MA'] = ta.DEMA(df['CLOSE'], timeperiod=SLOW_MA_TIMEPERIOD)
    
    return df

def read_json_files():
    with open('trade.json', 'r') as f1, open('portfolio.json', 'r') as f2:
        trade = json.load(f1)
        portfolio = json.load(f2)

    return trade, portfolio

def update_trade_file(trade):
    with open('trade.json', 'w') as f:
        json.dump(trade, f)

def update_portfolio_file(client, portfolio):
    portfolio['USDT_balance'] = float(client.get_asset_balance(asset='USDT')['free'])
    portfolio['BTC_balance'] = float(client.get_asset_balance(asset='BTC')['free'])
        
    with open('portfolio.json', 'w') as f:
        json.dump(portfolio, f)

def truncate(number, decimals):
    return math.floor(number * 10 ** decimals) / 10 ** decimals

def compute_strategy(client, df):
    trade, portfolio = read_json_files()

    if trade['entry_price']:

        # Sell (stop loss or take profit)
        if (df['CLOSE'].iloc[-1] < trade['highest_price'] * SL_THRESHOLD or
            df['CLOSE'].iloc[-1] > trade['entry_price'] * TP_THRESHOLD):

            trade['exit_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            trade['exit_price'] = df['CLOSE'].iloc[-1]

            btc_quantity = truncate(portfolio['BTC_balance'], PRECISION)
            order = client.create_order(symbol=SYMBOL, side='SELL', type='MARKET', quantity=btc_quantity)

            update_trades_file(trade)
            trade = dict.fromkeys(trade, 0)

        # Update highest price
        elif df['CLOSE'].iloc[-1] > trade['highest_price']:
            trade['highest_price'] = df['CLOSE'].iloc[-1]
                
    # Buy (bullish signal)
    elif (df['FAST_MA'].iloc[-1] > df['SLOW_MA'].iloc[-1] and 
          df['FAST_MA'].iloc[-2] < df['SLOW_MA'].iloc[-2]): 

        trade['entry_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        trade['entry_price'] = trade['highest_price'] = df['CLOSE'].iloc[-1]

        btc_quantity = truncate(portfolio['USDT_balance'] / trade['entry_price'], PRECISION)
        order = client.create_order(symbol=SYMBOL, side='BUY', type='MARKET', quantity=btc_quantity)

    update_trade_file(trade)
    update_portfolio_file(client, portfolio)

def handle_socket_message(client, df, twm, msg):
    kline = msg['k']

    if kline['x']:
        twm.stop()

        # Add last candle close price when closed
        df['CLOSE'].iloc[-1] = float(kline['c'])
        df = set_dema(df)

        compute_strategy(client, df)

def main():
    client = Client(api_key=config.BINANCE_API_KEY, api_secret=config.BINANCE_API_SECRET)
    df = get_data(client)

    # Process data
    df = df.loc[:, [4]]
    df = df.rename({4: 'CLOSE'}, axis=1)
    df['CLOSE'] = df['CLOSE'].astype('float64')
    
    # Use websocket to get last candle close price when closed
    twm = ThreadedWebsocketManager(api_key=config.BINANCE_API_KEY, api_secret=config.BINANCE_API_SECRET)
    twm.start()
    twm.start_kline_socket(callback=partial(handle_socket_message, client, df, twm), symbol=SYMBOL, interval=INTERVAL)

if __name__ == '__main__':
    main()




