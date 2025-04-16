import sqlite3
from pycoingecko import CoinGeckoAPI
import time
import pandas as pd

def fetch_btc_ohlcv():
    cg = CoinGeckoAPI()
    # CoinGecko returns prices in ms since epoch
    data = cg.get_coin_market_chart_by_id(id='bitcoin', vs_currency='usd', days='max')
    prices = pd.DataFrame(data['prices'], columns=['timestamp', 'price'])
    volumes = pd.DataFrame(data['total_volumes'], columns=['timestamp', 'volume'])
    # CoinGecko does not provide OHLC in free API, so we approximate using price series
    # We'll use price as close, and fill open/high/low as same value for now
    df = prices.copy()
    df = df.rename(columns={'price': 'close'})
    df['open'] = df['close']
    df['high'] = df['close']
    df['low'] = df['close']
    df['volume'] = volumes['volume']
    df['symbol'] = 'BTC'
    # Convert ms to seconds
    df['timestamp'] = (df['timestamp'] / 1000).astype(int)
    return df[['symbol', 'timestamp', 'open', 'high', 'low', 'close', 'volume']]

def insert_ohlcv_to_db(df, db_path='crypto_data.sqlite'):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    for _, row in df.iterrows():
        c.execute('''
            INSERT OR REPLACE INTO historical_prices (symbol, timestamp, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (row['symbol'], row['timestamp'], row['open'], row['high'], row['low'], row['close'], row['volume']))
    conn.commit()
    conn.close()

def main():
    df = fetch_btc_ohlcv()
    insert_ohlcv_to_db(df)
    print(f"Inserted {len(df)} BTC OHLCV records into the database.")

if __name__ == '__main__':
    main()
