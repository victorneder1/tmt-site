"""
Pre-process Anatel CSV/Excel files into aggregated SQLite database.

Supports two formats:
  1. Old CSVs: pivoted with month columns (YYYY-MM), "Grupo Econômico"
  2. New Excel: one row per record, "Grupo/Empresa", "Período", access count column
"""

import os
import glob
import sqlite3
import unicodedata
import pandas as pd

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "anatel.db")
BB_DIR = os.path.join(os.path.dirname(__file__), "Database", "Broadband")
MOB_DIR = os.path.join(os.path.dirname(__file__), "Database", "Mobile")
PORT_CSV = os.path.join(os.path.dirname(__file__), "Database", "CSV_PORTABILIDADE.csv")

# ── Operator mappings ──
# Old CSV: "Grupo Econômico" values
BB_OPS_OLD = {
    "TELEFONICA": "Vivo",
    "TELECOM AMERICAS": "Claro",
    "OI": "Nio",
    "BRISANET": "Brisanet",
    "GIGA MAIS FIBRA": "Giga+",
    "EB FIBRA": "Giga+",
    "VERO": "Vero",
    "BRASIL TECPAR": "Tecpar",
    "DESKTOP": "Desktop",
    "TELECOM ITALIA": "TIM",
    "UNIFIQUE": "Unifique",
}

MOB_OPS_OLD = {
    "TELEFONICA": "Vivo",
    "TELECOM AMERICAS": "Claro",
    "NEXTEL": "Claro",
    "TELECOM ITALIA": "TIM",
    "BRISANET": "Brisanet",
    "UNIFIQUE": "Unifique",
}

# New Excel: "Grupo/Empresa" values (exact match, case-insensitive)
BB_OPS_NEW = {
    "VIVO": "Vivo",
    "CLARO": "Claro",
    "OI": "Nio",
    "BRISANET": "Brisanet",
    "GIGA MAIS FIBRA": "Giga+",
    "VERO": "Vero",
    "BRASIL TECPAR": "Tecpar",
    "DESKTOP": "Desktop",
    "TIM": "TIM",
    "UNIFIQUE": "Unifique",
    "STARLINK": "Starlink",
}

MOB_OPS_NEW = {
    "VIVO": "Vivo",
    "CLARO": "Claro",
    "TIM": "TIM",
    "BRISANET": "Brisanet",
    "UNIFIQUE": "Unifique",
}


PORT_OPS = {
    "CLARO": "Claro",
    "TELEFONICA BRASIL": "Vivo",
    "OI S.A.": "Oi",
    "TIM S.A.": "TIM",
    "BRISANET SERVICOS DE TELECOMUNICACOES S.A.": "Brisanet",
    "UNIFIQUE TELECOMUNICACOES S/A": "Unifique",
    "NEXTEL TELECOM.": "Claro",
    "ALGAR TELECOM": "Algar",
    "SERCOMTEL": "Sercomtel",
}


# Empresa-level fallback: when Grupo Econômico is "OUTROS", check
# the Empresa column to recover operators that Anatel reclassified.
BB_EMPRESA_MAP = {
    "OI": "Nio",
    "BRASIL TECPAR": "Tecpar",
    "STARLINK": "Starlink",
}


def strip_accents(s):
    """Remove diacritics/accents from a string."""
    nfkd = unicodedata.normalize("NFKD", s)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def map_operator_exact(name, operator_map):
    """Map operator name using exact case-insensitive match, ignoring accents."""
    if not name or pd.isna(name):
        return "Others"
    key = strip_accents(str(name).strip().upper())
    if key in operator_map:
        return operator_map[key]
    return "Others"


def map_operator_with_empresa(grupo, empresa, grupo_map, empresa_map):
    """Map operator by Grupo Econômico first; if that yields 'Others',
    try the Empresa column against empresa_map (startswith match)."""
    op = map_operator_exact(grupo, grupo_map)
    if op != "Others":
        return op
    if not empresa or pd.isna(empresa):
        return "Others"
    key = strip_accents(str(empresa).strip().upper())
    # Try exact match first, then startswith
    for prefix, mapped in empresa_map.items():
        if key == prefix or key.startswith(prefix):
            return mapped
    return "Others"


# ── Broadband ──

def process_broadband():
    """Process all broadband files into aggregated monthly data with technology breakdown."""
    print("Processing Broadband data...")
    all_rows = []

    # 1. Old CSV files
    csv_files = sorted(glob.glob(os.path.join(BB_DIR, "Acessos_Banda_Larga_Fixa_*_Colunas.csv")))
    for fpath in csv_files:
        fname = os.path.basename(fpath)
        print(f"  Reading {fname}...")
        df = pd.read_csv(fpath, sep=";", encoding="utf-8-sig", dtype=str, low_memory=False)

        month_cols = [c for c in df.columns if c.startswith("20")]
        if not month_cols:
            continue

        # Normalize technology column: FTTH → "FTTH", others → "Other"
        tech_col = "Tecnologia" if "Tecnologia" in df.columns else None
        if tech_col:
            df["tech"] = df[tech_col].apply(lambda t: "FTTH" if str(t).strip().upper() == "FTTH" else "Other")
        else:
            df["tech"] = "Other"

        grupo_col = [c for c in df.columns if "Grupo" in c][0]
        empresa_col = "Empresa" if "Empresa" in df.columns else None
        id_cols = [grupo_col, "UF", "tech"]
        if empresa_col:
            id_cols.append(empresa_col)

        df = df[id_cols + month_cols]
        melted = df.melt(
            id_vars=id_cols,
            value_vars=month_cols,
            var_name="month",
            value_name="accesses",
        )
        melted["accesses"] = pd.to_numeric(melted["accesses"], errors="coerce").fillna(0).astype(int)
        if empresa_col:
            melted["operator"] = melted.apply(
                lambda r: map_operator_with_empresa(r[grupo_col], r[empresa_col], BB_OPS_OLD, BB_EMPRESA_MAP), axis=1
            )
        else:
            melted["operator"] = melted[grupo_col].apply(lambda g: map_operator_exact(g, BB_OPS_OLD))
        agg = melted.groupby(["operator", "UF", "month", "tech"], as_index=False)["accesses"].sum()
        all_rows.append(agg)

    # 2. New Excel files
    xlsx_files = sorted(glob.glob(os.path.join(BB_DIR, "*.xlsx")))
    for fpath in xlsx_files:
        fname = os.path.basename(fpath)
        if fname.startswith("~$"):
            continue
        print(f"  Reading {fname}...")
        df = pd.read_excel(fpath)

        acc_col = [c for c in df.columns if "Acessos" in c]
        if not acc_col:
            print(f"    Skipping {fname} - no Acessos column found")
            continue
        acc_col = acc_col[0]

        df["month"] = pd.to_datetime(df["Período"]).dt.strftime("%Y-%m")
        df["accesses"] = pd.to_numeric(df[acc_col], errors="coerce").fillna(0).astype(int)
        df["operator"] = df["Grupo/Empresa"].apply(lambda g: map_operator_exact(g, BB_OPS_NEW))

        # Technology
        tech_col = "Tecnologia" if "Tecnologia" in df.columns else None
        if tech_col:
            df["tech"] = df[tech_col].apply(lambda t: "FTTH" if str(t).strip().upper() == "FTTH" else "Other")
        else:
            df["tech"] = "Other"

        agg = df.groupby(["operator", "UF", "month", "tech"], as_index=False)["accesses"].sum()
        all_rows.append(agg)

    result = pd.concat(all_rows, ignore_index=True)
    # Limit to Jan 2015 onwards
    result = result[result["month"] >= "2015-01"]
    result = result.groupby(["operator", "UF", "month", "tech"], as_index=False)["accesses"].sum()
    result = result.sort_values(["month", "operator", "UF", "tech"])

    # ── Data corrections ──
    # TIM Nov 2018: source data has wrong values for RJ and SP.
    # Correct total should be 479019; distribute deficit to RJ and SP
    # proportionally based on Oct/Dec averages.
    mask_tim_nov = (result["operator"] == "TIM") & (result["month"] == "2018-11")
    current_total = result.loc[mask_tim_nov, "accesses"].sum()
    target_total = 479019
    deficit = target_total - current_total

    if deficit > 0:
        mask_rj = mask_tim_nov & (result["UF"] == "RJ")
        mask_sp = mask_tim_nov & (result["UF"] == "SP")
        # Split deficit proportionally (~43% RJ, ~57% SP based on neighbors)
        rj_share = round(deficit * 0.43)
        sp_share = deficit - rj_share
        result.loc[mask_rj, "accesses"] = result.loc[mask_rj, "accesses"] + rj_share
        result.loc[mask_sp, "accesses"] = result.loc[mask_sp, "accesses"] + sp_share

    print(f"  Total broadband rows: {len(result)}")
    return result


# ── Mobile ──

def process_mobile():
    """Process all mobile files into aggregated monthly data."""
    print("Processing Mobile data...")
    all_rows = []

    csv_files = sorted(glob.glob(os.path.join(MOB_DIR, "Acessos_Telefonia_Movel_*_Colunas.csv")))

    # 1. Old CSV files (pivoted month columns)
    for fpath in csv_files:
        fname = os.path.basename(fpath)
        print(f"  Reading {fname}...")
        df = pd.read_csv(fpath, sep=";", encoding="utf-8-sig", dtype=str, low_memory=False)

        month_cols = [c for c in df.columns if c.startswith("20")]
        if not month_cols:
            continue

        grupo_col = [c for c in df.columns if "Grupo" in c][0]
        billing_col = [c for c in df.columns if "Cobran" in c][0]

        id_cols = [grupo_col, "UF", billing_col, "Tipo de Produto"]
        df = df[id_cols + month_cols]
        melted = df.melt(
            id_vars=id_cols,
            value_vars=month_cols,
            var_name="month",
            value_name="accesses",
        )
        melted["accesses"] = pd.to_numeric(melted["accesses"], errors="coerce").fillna(0).astype(int)
        melted["operator"] = melted[grupo_col].apply(lambda g: map_operator_exact(g, MOB_OPS_OLD))

        # Classify: exclude M2M* and PONTO_DE_SERVICO from postpaid
        is_post = melted[billing_col].apply(lambda b: "s-pago" in str(b))
        prod = melted["Tipo de Produto"].fillna("")
        is_excluded = prod.str.contains("M2M", na=False) | prod.str.startswith("PONTO", na=False)
        melted["segment"] = "Prepaid"
        melted.loc[is_post & ~is_excluded, "segment"] = "Postpaid"
        melted.loc[is_post & is_excluded, "segment"] = "Excluded"

        agg = melted.groupby(["operator", "UF", "month", "segment"], as_index=False)["accesses"].sum()
        all_rows.append(agg)

    # 2. Non-standard CSV files (Ano/Mês format instead of pivoted month columns)
    all_mob_csvs = set(glob.glob(os.path.join(MOB_DIR, "*.csv")))
    standard_csvs = set(csv_files)
    extra_csvs = sorted(all_mob_csvs - standard_csvs)
    for fpath in extra_csvs:
        fname = os.path.basename(fpath)
        print(f"  Reading {fname} (row-based format)...")
        df = pd.read_csv(fpath, sep=";", encoding="utf-8-sig", dtype=str, low_memory=False)

        # Build month from Ano + Mês columns
        ano_col = [c for c in df.columns if c == "Ano"][0]
        mes_col = [c for c in df.columns if "s" in c and c not in ("Acessos",)
                   and c.startswith("M")][0]
        df["month"] = df[ano_col].str.strip() + "-" + df[mes_col].str.strip().str.zfill(2)

        acc_col = [c for c in df.columns if "Acessos" in c][0]
        df["accesses"] = pd.to_numeric(df[acc_col], errors="coerce").fillna(0).astype(int)

        grupo_col = [c for c in df.columns if "Grupo" in c][0]
        billing_col = [c for c in df.columns if "Cobran" in c][0]
        df["operator"] = df[grupo_col].apply(lambda g: map_operator_exact(g, MOB_OPS_OLD))

        is_post = df[billing_col].apply(lambda b: "s-pago" in str(b))
        prod = df["Tipo de Produto"].fillna("")
        is_excluded = prod.str.contains("M2M", na=False) | prod.str.startswith("PONTO", na=False)
        df["segment"] = "Prepaid"
        df.loc[is_post & ~is_excluded, "segment"] = "Postpaid"
        df.loc[is_post & is_excluded, "segment"] = "Excluded"

        agg = df.groupby(["operator", "UF", "month", "segment"], as_index=False)["accesses"].sum()
        all_rows.append(agg)

    result = pd.concat(all_rows, ignore_index=True)
    result = result.groupby(["operator", "UF", "month", "segment"], as_index=False)["accesses"].sum()
    result = result.sort_values(["month", "operator", "UF", "segment"])

    print(f"  Total mobile rows: {len(result)}")
    return result


def process_portability():
    """Process portability CSV into aggregated pair-wise monthly data (SMP only)."""
    print("Processing Portability data...")
    if not os.path.exists(PORT_CSV):
        print("  Portability CSV not found, skipping.")
        return None
    df = pd.read_csv(PORT_CSV, sep=";", encoding="utf-8-sig", dtype=str, low_memory=False)

    # Filter SMP only
    df = df[df["SG_SERVICO"] == "SMP"]

    df["quantity"] = pd.to_numeric(df["QT_PORTABILIDADE_EFETIVADA"], errors="coerce").fillna(0).astype(int)
    df["month"] = df["AM_EFETIVACAO"]
    df["UF"] = df["SG_UF"]

    df["giver"] = df["NO_PRESTADORA_DOADORA"].apply(lambda g: map_operator_exact(g, PORT_OPS))
    df["receiver"] = df["NO_PRESTADORA_RECEPTORA"].apply(lambda r: map_operator_exact(r, PORT_OPS))

    result = df.groupby(["giver", "receiver", "month", "UF"], as_index=False)["quantity"].sum()
    result = result.sort_values(["month", "giver", "receiver", "UF"])

    print(f"  Total portability rows: {len(result)}")
    return result


def save_to_db(broadband_df, mobile_df, portability_df=None):
    """Save aggregated data to SQLite."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    conn.execute("DROP TABLE IF EXISTS broadband")
    conn.execute("DROP TABLE IF EXISTS mobile")

    broadband_df.to_sql("broadband", conn, index=False)
    mobile_df.to_sql("mobile", conn, index=False)

    conn.execute("CREATE INDEX idx_bb_month ON broadband(month)")
    conn.execute("CREATE INDEX idx_bb_operator ON broadband(operator)")
    conn.execute("CREATE INDEX idx_bb_uf ON broadband(UF)")
    conn.execute("CREATE INDEX idx_bb_tech ON broadband(tech)")
    conn.execute("CREATE INDEX idx_mob_month ON mobile(month)")
    conn.execute("CREATE INDEX idx_mob_operator ON mobile(operator)")
    conn.execute("CREATE INDEX idx_mob_uf ON mobile(UF)")
    conn.execute("CREATE INDEX idx_mob_segment ON mobile(segment)")

    if portability_df is not None:
        conn.execute("DROP TABLE IF EXISTS portability")
        portability_df.to_sql("portability", conn, index=False)
        conn.execute("CREATE INDEX idx_port_month ON portability(month)")
        conn.execute("CREATE INDEX idx_port_giver ON portability(giver)")
        conn.execute("CREATE INDEX idx_port_receiver ON portability(receiver)")
        conn.execute("CREATE INDEX idx_port_uf ON portability(UF)")

    conn.commit()
    conn.close()
    print(f"Database saved to {DB_PATH}")


def save_portability_to_db(portability_df):
    """Save only the portability table (preserves existing broadband/mobile)."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("DROP TABLE IF EXISTS portability")
    portability_df.to_sql("portability", conn, index=False)
    conn.execute("CREATE INDEX idx_port_month ON portability(month)")
    conn.execute("CREATE INDEX idx_port_giver ON portability(giver)")
    conn.execute("CREATE INDEX idx_port_receiver ON portability(receiver)")
    conn.execute("CREATE INDEX idx_port_uf ON portability(UF)")
    conn.commit()
    conn.close()
    print(f"Portability table saved to {DB_PATH}")


if __name__ == "__main__":
    bb = process_broadband()
    mob = process_mobile()
    port = process_portability()
    save_to_db(bb, mob, port)
    print("Done!")
