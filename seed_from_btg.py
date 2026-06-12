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

    # Espelhar o subconjunto TMT do btg10sim: documentos que existem no destino,
    # mas nao existem mais na origem, devem sair para nao distorcer "latest filing".
    source_protocols = {d["protocol"] for d in docs}
    cvm_codes = tuple(str(c.get("cvm_code")) for c in companies if c.get("cvm_code"))
    ph_cvm = ",".join("?" * len(cvm_codes))
    existing = dst.execute(
        f"SELECT protocol FROM documents WHERE cvm_code IN ({ph_cvm})", cvm_codes
    ).fetchall()
    stale_protocols = [row[0] for row in existing if row[0] not in source_protocols]
    if stale_protocols:
        ph_stale = ",".join("?" * len(stale_protocols))
        dst.execute(f"DELETE FROM movements WHERE protocol IN ({ph_stale})", stale_protocols)
        dst.execute(f"DELETE FROM documents WHERE protocol IN ({ph_stale})", stale_protocols)
        print(f"Documentos stale removidos: {len(stale_protocols)}")

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

    # Normalizar ticker e sector para cada empresa segundo companies_bz.json.
    # O btg10sim pode usar tickers diferentes (ex: BRIT3 vs BRST3) para a mesma
    # empresa (mesmo cvm_code). Garantimos que o tmt-site use sempre os valores
    # configurados localmente.
    cvm_to_company = {c["cvm_code"]: c for c in companies if c.get("cvm_code")}
    total_doc_fixes = 0
    total_mov_fixes = 0
    for cvm_code, company in cvm_to_company.items():
        ticker = company.get("ticker", "")
        sector = company.get("sector", "")
        if not ticker:
            continue
        rd = dst.execute(
            "UPDATE documents SET ticker = ?, sector = ? WHERE cvm_code = ? AND (ticker != ? OR sector != ?)",
            (ticker, sector, str(cvm_code), ticker, sector),
        )
        rm = dst.execute(
            "UPDATE movements SET ticker = ?, sector = ? "
            "WHERE protocol IN (SELECT protocol FROM documents WHERE cvm_code = ?) "
            "AND (ticker != ? OR sector != ?)",
            (ticker, sector, str(cvm_code), ticker, sector),
        )
        if rd.rowcount or rm.rowcount:
            print(f"Normalizado {company['name']}: {rd.rowcount} docs, {rm.rowcount} movimentos -> ticker={ticker} sector={sector}")
        total_doc_fixes += rd.rowcount
        total_mov_fixes += rm.rowcount
    if total_doc_fixes or total_mov_fixes:
        print(f"Total normalizações: {total_doc_fixes} documentos, {total_mov_fixes} movimentos")

    dst.commit()
    src.close()
    dst.close()
    print(f"\nDB salvo em: {TMT_DB.resolve()}")
    print("Próximo passo: python upload_db.py")

if __name__ == "__main__":
    main()
