"""
Atualiza dados Telecom: processa CSVs locais e faz upload do DB para o servidor.

Uso:
    python update_telecom.py
"""

import os
import sys
import requests
import process_data_telecom

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


def _load_env():
    env_path = os.path.join(SCRIPT_DIR, ".env")
    if os.path.exists(env_path):
        with open(env_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, val = line.split("=", 1)
                    os.environ.setdefault(key.strip(), val.strip())


_load_env()

SERVER_URL = os.environ.get("SERVER_URL", "http://localhost:8080")
UPLOAD_KEY = os.environ.get("UPLOAD_KEY", "change-me-before-deploy")


def main():
    # 1. Processar CSVs
    print("=" * 50)
    print("STEP 1: Processing CSVs...")
    print("=" * 50)

    bb = process_data_telecom.process_broadband()
    mob = process_data_telecom.process_mobile()
    port = process_data_telecom.process_portability()
    process_data_telecom.save_to_db(bb, mob, port)

    db_path = process_data_telecom.DB_PATH
    if not os.path.exists(db_path):
        print(f"ERROR: DB not found at {db_path}")
        sys.exit(1)

    size_mb = os.path.getsize(db_path) / (1024 * 1024)
    print(f"\nDB ready: {db_path} ({size_mb:.1f} MB)")

    # 2. Upload para servidor
    print()
    print("=" * 50)
    print("STEP 2: Uploading to server...")
    print("=" * 50)

    url = f"{SERVER_URL}/api/upload-anatel"
    try:
        with open(db_path, "rb") as f:
            resp = requests.post(
                url,
                data={"key": UPLOAD_KEY},
                files={"anatel_db": f},
                timeout=120,
            )

        if resp.status_code == 200:
            print(f"OK: {resp.json()}")
        else:
            print(f"FAILED ({resp.status_code}): {resp.json()}")
            sys.exit(1)

    except requests.ConnectionError:
        print(f"ERROR: Could not connect to {url}")
        sys.exit(1)

    print()
    print("Done! Telecom data updated successfully.")


if __name__ == "__main__":
    main()
