import sqlite3
import yfinance as yf
import pandas as pd

def fetch_btc_ohlcv():
    # Download daily BTC-USD history from Yahoo Finance
    btc = yf.download('BTC-USD', interval='1d', progress=False)
    btc = btc.reset_index()
    # Fix: Proper timestamp conversion from datetime to unix seconds
    if pd.api.types.is_datetime64_any_dtype(btc['Date']):
        btc['timestamp'] = btc['Date'].astype('int64') // 10**9
    else:
        btc['timestamp'] = btc['Date'].apply(lambda x: int(pd.Timestamp(x).timestamp()))
    btc['symbol'] = 'BTC'
    btc['Volume'] = btc['Volume'].fillna(0)
    # Ensure correct column types
    btc = btc[['symbol', 'timestamp', 'Open', 'High', 'Low', 'Close', 'Volume']]
    btc = btc.astype({
        'symbol': str,
        'timestamp': int,
        'Open': float,
        'High': float,
        'Low': float,
        'Close': float,
        'Volume': float
    })
    return btc

def insert_ohlcv_to_db(df, db_path='crypto_data.sqlite'):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    for row in df.itertuples(index=False, name=None):
        c.execute('''
            INSERT OR REPLACE INTO historical_prices (symbol, timestamp, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', row)
    conn.commit()
    conn.close()

def main():
    df = fetch_btc_ohlcv()
    insert_ohlcv_to_db(df)
    print(f"Inserted {len(df)} BTC OHLCV records into the database.")

if __name__ == '__main__':
    main()
