"""
Importa pares do servidor de produção para o banco local.

Uso:
    python import_pairs.py
"""

import os
import sqlite3
import requests

SERVER_URL = "https://web-production-9e57a.up.railway.app"

print(f"Buscando pares de {SERVER_URL}/api/pairs ...")
r = requests.get(f"{SERVER_URL}/api/pairs", timeout=15)
r.raise_for_status()

pairs = r.json()
if isinstance(pairs, dict) and "error" in pairs:
    print(f"Erro do servidor: {pairs['error']}")
    exit(1)

print(f"{len(pairs)} pares encontrados.")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("DATA_DIR", os.path.join(BASE_DIR, "data"))
DB_PATH = os.path.join(DATA_DIR, "pairs.db")

os.makedirs(DATA_DIR, exist_ok=True)
conn = sqlite3.connect(DB_PATH)

conn.execute("""
    CREATE TABLE IF NOT EXISTS pairs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        long_ticker TEXT NOT NULL,
        short_ticker TEXT NOT NULL,
        entry_price_long TEXT NOT NULL,
        entry_price_short TEXT NOT NULL,
        entry_date TEXT NOT NULL,
        inception_date TEXT,
        status TEXT NOT NULL DEFAULT 'open',
        closed_date TEXT,
        close_price_long TEXT,
        close_price_short TEXT,
        sort_order INTEGER DEFAULT 0,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
""")

conn.execute("DELETE FROM pairs")

for p in pairs:
    # /api/pairs retorna preços atuais — salvamos os de entrada
    entry_long = p.get("entry_price_long")
    entry_short = p.get("entry_price_short")

    import json
    if isinstance(entry_long, list):
        entry_long = json.dumps(entry_long)
    if isinstance(entry_short, list):
        entry_short = json.dumps(entry_short)

    long_ticker = p.get("long_ticker")
    short_ticker = p.get("short_ticker")
    if isinstance(long_ticker, list):
        long_ticker = json.dumps(long_ticker)
    if isinstance(short_ticker, list):
        short_ticker = json.dumps(short_ticker)

    close_long = p.get("close_price_long")
    close_short = p.get("close_price_short")
    if isinstance(close_long, list):
        close_long = json.dumps(close_long)
    if isinstance(close_short, list):
        close_short = json.dumps(close_short)

    conn.execute(
        """INSERT INTO pairs
           (id, long_ticker, short_ticker, entry_price_long, entry_price_short,
            entry_date, inception_date, status, closed_date, close_price_long,
            close_price_short, sort_order, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [
            p.get("id"), long_ticker, short_ticker,
            entry_long, entry_short,
            p.get("entry_date"), p.get("inception_date"),
            p.get("status", "open"), p.get("closed_date"),
            close_long, close_short,
            p.get("sort_order", 0), p.get("created_at"),
        ],
    )

conn.commit()
conn.close()
print(f"Importação concluída. {len(pairs)} pares salvos em {DB_PATH}")
