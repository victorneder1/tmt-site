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

    # Copiar movimentos — limpa primeiro para evitar duplicatas
    protocols = tuple(d["protocol"] for d in docs)
    ph2  = ",".join("?" * len(protocols))
    # Remove movimentos existentes para esses protocolos antes de reinserir
    dst.execute(f"DELETE FROM movements WHERE protocol IN ({ph2})", protocols)
    movs = src.execute(
        f"SELECT * FROM movements WHERE protocol IN ({ph2})", protocols
    ).fetchall()
    if movs:
        mov_cols = [c for c in movs[0].keys() if c != "id"]  # ignora id para evitar conflitos
        col_str2 = ", ".join(mov_cols)
        val_str2 = ", ".join("?" * len(mov_cols))
        dst.executemany(
            f"INSERT INTO movements ({col_str2}) VALUES ({val_str2})",
            [tuple(m[c] for c in mov_cols) for m in movs]
        )
    print(f"Movimentos copiados: {len(movs)}")

    # Remover documentos e movimentos de Jan/26 para LWSA e MBRF
    # (esses documentos existem no btg10sim mas não foram processados em produção,
    # e queremos manter consistência entre os dois sites)
    EXCLUDE = [
        ("LWSA3", "2026-01%"),
        ("MBRF3", "2026-01%"),
    ]
    for ticker, ref_pattern in EXCLUDE:
        protocols = [
            r[0] for r in dst.execute(
                "SELECT protocol FROM documents WHERE ticker = ? AND reference_date LIKE ?",
                (ticker, ref_pattern),
            ).fetchall()
        ]
        for p in protocols:
            dst.execute("DELETE FROM movements WHERE protocol = ?", (p,))
            dst.execute("DELETE FROM documents WHERE protocol = ?", (p,))
        if protocols:
            print(f"Excluído Jan/26 de {ticker}: {len(protocols)} documento(s)")

    dst.commit()
    src.close()
    dst.close()
    print(f"\nDB salvo em: {TMT_DB.resolve()}")
    print("Próximo passo: python upload_db.py")

if __name__ == "__main__":
    main()
