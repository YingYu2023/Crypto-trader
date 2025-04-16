import sqlite3

def create_db(db_path='crypto_data.sqlite'):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    # Historical Prices Table
    c.execute('''
        CREATE TABLE IF NOT EXISTS historical_prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL
        )
    ''')
    # News Articles Table
    c.execute('''
        CREATE TABLE IF NOT EXISTS news_articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT,
            title TEXT,
            url TEXT,
            published_at TEXT,
            summary TEXT,
            raw_json TEXT
        )
    ''')
    # Social Media Posts Table
    c.execute('''
        CREATE TABLE IF NOT EXISTS social_media_posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            platform TEXT,
            author TEXT,
            content TEXT,
            url TEXT,
            created_utc INTEGER,
            raw_json TEXT
        )
    ''')
    # On-chain Metrics Table
    c.execute('''
        CREATE TABLE IF NOT EXISTS onchain_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            metric TEXT,
            value REAL,
            timestamp INTEGER,
            raw_json TEXT
        )
    ''')
    conn.commit()
    conn.close()

if __name__ == '__main__':
    create_db()
