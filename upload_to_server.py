"""
Upload planilhas para o servidor remoto.

Uso:
    python upload_to_server.py

Configuração via variáveis de ambiente ou editando as constantes abaixo:
    SERVER_URL  - URL do servidor (ex: https://meusite.com)
    UPLOAD_KEY  - Chave de autenticação para upload
"""

import os
import sys
import requests

SCRIPT_DIR_ENV = os.path.dirname(os.path.abspath(__file__))


def _load_env():
    """Load .env file if it exists (simple key=value parser)."""
    env_path = os.path.join(SCRIPT_DIR_ENV, ".env")
    if os.path.exists(env_path):
        with open(env_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, val = line.split("=", 1)
                    os.environ.setdefault(key.strip(), val.strip())


_load_env()

# --- CONFIGURAÇÃO ---
# Lê de .env ou variáveis de ambiente
SERVER_URL = os.environ.get("SERVER_URL", "http://localhost:8080")
UPLOAD_KEY = os.environ.get("UPLOAD_KEY", "change-me-before-deploy")

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SOFTWARE_FILE = os.path.join(SCRIPT_DIR, "Screening_VisibleAlpha_Software_site.xlsx")
ITSERVICES_FILE = os.path.join(SCRIPT_DIR, "Screening_VisibleAlpha_ITServices_site.xlsx")


def upload():
    url = f"{SERVER_URL}/api/upload"

    files = {}
    if os.path.exists(SOFTWARE_FILE):
        files["software"] = open(SOFTWARE_FILE, "rb")
    if os.path.exists(ITSERVICES_FILE):
        files["itservices"] = open(ITSERVICES_FILE, "rb")

    if not files:
        print("ERROR: No Excel files found.")
        sys.exit(1)

    try:
        resp = requests.post(
            url,
            data={"key": UPLOAD_KEY},
            files=files,
            timeout=60,
        )

        if resp.status_code == 200:
            print(f"OK: {resp.json()}")
        else:
            print(f"FAILED ({resp.status_code}): {resp.json()}")
            sys.exit(1)

    except requests.ConnectionError:
        print(f"ERROR: Could not connect to {url}")
        sys.exit(1)
    finally:
        for f in files.values():
            f.close()


if __name__ == "__main__":
    upload()
