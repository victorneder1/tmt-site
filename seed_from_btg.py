"""
seed_from_btg.py — Copia documentos e movimentos das 9 empresas TMT
do DB do btg10sim para o DB do tmt-site.

Uso:
    python seed_from_btg.py
"""
import json
import sqlite3
from pathlib import Path

BTG_DB   = Path("../btg10sim/data/corporate_bz.db")
TMT_DB   = Path("data/corporate_bz.db")
TMT_JSON = Path("companies_bz.json")

def main():
    if not BTG_DB.exists():
        print(f"ERRO: DB do btg10sim não encontrado em {BTG_DB}")
        return
    if not TMT_JSON.exists():
        print(f"ERRO: {TMT_JSON} não encontrado")
        return

    companies = json.loads(TMT_JSON.read_text(encoding="utf-8"))
    aliases = tuple(c["name"] for c in companies)
    print(f"Empresas TMT: {aliases}")

    TMT_DB.parent.mkdir(parents=True, exist_ok=True)

    src = sqlite3.connect(BTG_DB)
    dst = sqlite3.connect(TMT_DB)
    src.row_factory = sqlite3.Row

    # Garantir que as tabelas existem no destino
    schema = src.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").fetchall()
    for row in schema:
        if row[0]:
            dst.execute(row[0].replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS"))
    dst.commit()

    # Copiar documentos
    ph = ",".join("?" * len(aliases))
    docs = src.execute(
        f"SELECT * FROM documents WHERE company_alias IN ({ph})", aliases
    ).fetchall()
    if not docs:
        print("Nenhum documento encontrado para as empresas TMT no btg10sim.")
        src.close(); dst.close(); return

    doc_cols = docs[0].keys()
    col_str  = ", ".join(doc_cols)
    val_str  = ", ".join("?" * len(doc_cols))
    dst.executemany(
        f"INSERT OR REPLACE INTO documents ({col_str}) VALUES ({val_str})",
        [tuple(d) for d in docs]
    )
    print(f"Documentos copiados: {len(docs)}")

    # Copiar movimentos
    protocols = tuple(d["protocol"] for d in docs)
    ph2  = ",".join("?" * len(protocols))
    movs = src.execute(
        f"SELECT * FROM movements WHERE protocol IN ({ph2})", protocols
    ).fetchall()
    if movs:
        mov_cols = movs[0].keys()
        col_str2 = ", ".join(mov_cols)
        val_str2 = ", ".join("?" * len(mov_cols))
        dst.executemany(
            f"INSERT OR REPLACE INTO movements ({col_str2}) VALUES ({val_str2})",
            [tuple(m) for m in movs]
        )
    print(f"Movimentos copiados: {len(movs)}")

    dst.commit()
    src.close()
    dst.close()
    print(f"\nDB salvo em: {TMT_DB.resolve()}")
    print("Próximo passo: python upload_db.py")

if __name__ == "__main__":
    main()
