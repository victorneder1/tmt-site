"""
upload_db.py — Envia o corporate_bz.db local para produção.

Uso:
    python upload_db.py
"""
import os
import sys

try:
    import requests
except ImportError:
    print("Instale requests: pip install requests")
    sys.exit(1)

DB_PATH    = "data/corporate_bz.db"
SERVER_URL = os.environ.get("SERVER_URL", "").rstrip("/")
UPLOAD_KEY = os.environ.get("UPLOAD_KEY", "")

if not SERVER_URL:
    SERVER_URL = input("URL do servidor (ex: https://btgtmt.com): ").strip().rstrip("/")
if not UPLOAD_KEY:
    UPLOAD_KEY = input("UPLOAD_KEY: ").strip()

if not os.path.exists(DB_PATH):
    print(f"ERRO: {DB_PATH} não encontrado. Rode seed_from_btg.py primeiro.")
    sys.exit(1)

url = f"{SERVER_URL}/api/upload-corporate-db"
print(f"Enviando {DB_PATH} para {url} ...")

with open(DB_PATH, "rb") as f:
    resp = requests.post(
        url,
        headers={"X-Upload-Key": UPLOAD_KEY},
        files={"file": ("corporate_bz.db", f, "application/octet-stream")},
        timeout=120,
    )

print(f"Status: {resp.status_code}")
print(resp.json())
