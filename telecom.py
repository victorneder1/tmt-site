"""
Telecom Dashboard Blueprint — serves Anatel broadband and mobile data.
"""

import os
import sqlite3
from flask import Blueprint, render_template, jsonify, request
from telecom_comps import invalidate_telco_comps_cache, load_telco_comps

telecom_bp = Blueprint("telecom", __name__, url_prefix="/telecom")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("DATA_DIR", os.path.join(BASE_DIR, "data"))
DB_PATH = os.path.join(DATA_DIR, "anatel.db")

UF_NAMES = {
    "AC": "Acre", "AL": "Alagoas", "AM": "Amazonas", "AP": "Amapá",
    "BA": "Bahia", "CE": "Ceará", "DF": "Distrito Federal", "ES": "Espírito Santo",
    "GO": "Goiás", "MA": "Maranhão", "MG": "Minas Gerais", "MS": "Mato Grosso do Sul",
    "MT": "Mato Grosso", "PA": "Pará", "PB": "Paraíba", "PE": "Pernambuco",
    "PI": "Piauí", "PR": "Paraná", "RJ": "Rio de Janeiro", "RN": "Rio Grande do Norte",
    "RO": "Rondônia", "RR": "Roraima", "RS": "Rio Grande do Sul", "SC": "Santa Catarina",
    "SE": "Sergipe", "SP": "São Paulo", "TO": "Tocantins",
}


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# ── Page ──

@telecom_bp.route("/")
def index():
    return render_template("telecom/index.html")


@telecom_bp.route("/api/comps")
def api_comps():
    try:
        return jsonify(load_telco_comps())
    except FileNotFoundError as exc:
        return jsonify({"error": str(exc)}), 503
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@telecom_bp.route("/api/upload-comps-files", methods=["POST"])
def api_upload_comps_files():
    from app import UPLOAD_KEY
    key = request.headers.get("X-Upload-Key") or request.form.get("key")
    if key != UPLOAD_KEY:
        return jsonify({"error": "Unauthorized"}), 403

    saved = []
    file_map = {
        "big_telcos":   "Big Telcos Data.xlsx",
        "daily_comps":  "daily comps template 2025 - site telecom.xlsx",
        "valuation":    "telecom_comps_2026_2027.json",
        "comps_cache":  "telco_comps_cache.json",
    }
    for field, filename in file_map.items():
        f = request.files.get(field)
        if f and f.filename:
            os.makedirs(DATA_DIR, exist_ok=True)
            f.save(os.path.join(DATA_DIR, filename))
            saved.append(filename)

    if not saved:
        return jsonify({"error": "No files provided"}), 400

    invalidate_telco_comps_cache(remove_disk=any(name != "telco_comps_cache.json" for name in saved))
    return jsonify({"status": "ok", "saved": saved})


# ── API: Broadband ──

@telecom_bp.route("/api/broadband")
def api_broadband():
    conn = get_db()
    uf = request.args.get("uf", "")
    month_from = request.args.get("from", "")
    month_to = request.args.get("to", "")
    tech = request.args.get("tech", "")

    query = "SELECT operator, month, SUM(accesses) as accesses FROM broadband WHERE 1=1"
    params = []

    if uf:
        query += " AND UF = ?"
        params.append(uf)
    if month_from:
        query += " AND month >= ?"
        params.append(month_from)
    if month_to:
        query += " AND month <= ?"
        params.append(month_to)
    if tech:
        query += " AND tech = ?"
        params.append(tech)

    query += " GROUP BY operator, month ORDER BY month, operator"
    rows = conn.execute(query, params).fetchall()
    conn.close()

    data = [{"operator": r["operator"], "month": r["month"], "accesses": r["accesses"]} for r in rows]
    return jsonify(data)


@telecom_bp.route("/api/broadband/months")
def api_broadband_months():
    conn = get_db()
    rows = conn.execute("SELECT DISTINCT month FROM broadband ORDER BY month").fetchall()
    conn.close()
    return jsonify([r["month"] for r in rows])


@telecom_bp.route("/api/broadband/states")
def api_broadband_states():
    conn = get_db()
    rows = conn.execute("SELECT DISTINCT UF FROM broadband ORDER BY UF").fetchall()
    conn.close()
    states = [{"code": r["UF"], "name": UF_NAMES.get(r["UF"], r["UF"])} for r in rows]
    return jsonify(states)


# ── API: Mobile ──

@telecom_bp.route("/api/mobile")
def api_mobile():
    conn = get_db()
    uf = request.args.get("uf", "")
    month_from = request.args.get("from", "")
    month_to = request.args.get("to", "")
    segment = request.args.get("segment", "")

    query = "SELECT operator, month, segment, SUM(accesses) as accesses FROM mobile WHERE segment != 'Excluded'"
    params = []

    if uf:
        query += " AND UF = ?"
        params.append(uf)
    if month_from:
        query += " AND month >= ?"
        params.append(month_from)
    if month_to:
        query += " AND month <= ?"
        params.append(month_to)
    if segment:
        query += " AND segment = ?"
        params.append(segment)

    query += " GROUP BY operator, month, segment ORDER BY month, operator, segment"
    rows = conn.execute(query, params).fetchall()
    conn.close()

    data = [{"operator": r["operator"], "month": r["month"], "segment": r["segment"], "accesses": r["accesses"]} for r in rows]
    return jsonify(data)


@telecom_bp.route("/api/mobile/months")
def api_mobile_months():
    conn = get_db()
    rows = conn.execute("SELECT DISTINCT month FROM mobile ORDER BY month").fetchall()
    conn.close()
    return jsonify([r["month"] for r in rows])


# ── API: Portability ──

@telecom_bp.route("/api/portability")
def api_portability():
    conn = get_db()
    uf = request.args.get("uf", "")
    month_from = request.args.get("from", "")
    month_to = request.args.get("to", "")

    query = "SELECT giver, receiver, month, SUM(quantity) as quantity FROM portability WHERE 1=1"
    params = []

    if uf:
        query += " AND UF = ?"
        params.append(uf)
    if month_from:
        query += " AND month >= ?"
        params.append(month_from)
    if month_to:
        query += " AND month <= ?"
        params.append(month_to)

    query += " GROUP BY giver, receiver, month ORDER BY month, giver, receiver"
    rows = conn.execute(query, params).fetchall()
    conn.close()

    data = [{"giver": r["giver"], "receiver": r["receiver"], "month": r["month"], "quantity": r["quantity"]} for r in rows]
    return jsonify(data)


@telecom_bp.route("/api/portability/months")
def api_portability_months():
    conn = get_db()
    rows = conn.execute("SELECT DISTINCT month FROM portability ORDER BY month").fetchall()
    conn.close()
    return jsonify([r["month"] for r in rows])


@telecom_bp.route("/api/portability/operators")
def api_portability_operators():
    conn = get_db()
    rows = conn.execute("""
        SELECT DISTINCT op FROM (
            SELECT giver AS op FROM portability
            UNION
            SELECT receiver AS op FROM portability
        ) ORDER BY op
    """).fetchall()
    conn.close()
    return jsonify([r["op"] for r in rows])
