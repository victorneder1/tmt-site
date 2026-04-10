import json
import os
from functools import wraps
from datetime import datetime, timedelta
from flask import Flask, render_template, jsonify, request, redirect, url_for, session
from werkzeug.utils import secure_filename

from data_parser import parse_software_comps, parse_itservices_comps
import pairs_service
import process_data_telecom
from telecom import telecom_bp
from corporate import create_tracker_bp

app = Flask(__name__)
app.register_blueprint(telecom_bp)
app.register_blueprint(create_tracker_bp())
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret-key-change-me")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("DATA_DIR", os.path.join(BASE_DIR, "data"))
os.makedirs(DATA_DIR, exist_ok=True)
SOFTWARE_FILE = os.path.join(DATA_DIR, "Screening_VisibleAlpha_Software_site.xlsx")
ITSERVICES_FILE = os.path.join(DATA_DIR, "Screening_VisibleAlpha_ITServices_site.xlsx")
SOFTWARE_LEGACY_FILE = os.path.join(BASE_DIR, "Screening_VisibleAlpha_Software_site.xlsx")
ITSERVICES_LEGACY_FILE = os.path.join(BASE_DIR, "Screening_VisibleAlpha_ITServices_site.xlsx")
SOFTWARE_CACHE_FILE = os.path.join(DATA_DIR, "Screening_VisibleAlpha_Software_site.json")
ITSERVICES_CACHE_FILE = os.path.join(DATA_DIR, "Screening_VisibleAlpha_ITServices_site.json")
LAST_UPDATED_CACHE_FILE = os.path.join(DATA_DIR, "Screening_VisibleAlpha_last_updated.json")

# Upload key for authentication — change this to a strong secret before deploying
UPLOAD_KEY = os.environ.get("UPLOAD_KEY", "change-me-before-deploy")

ALLOWED_FILES = {
    "software": os.path.basename(SOFTWARE_FILE),
    "itservices": os.path.basename(ITSERVICES_FILE),
}


def _load_json_file(path):
    with open(path, encoding="utf-8-sig") as f:
        return json.load(f)


def _write_json_file(path, payload):
    temp_path = f"{path}.tmp"
    with open(temp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)
    os.replace(temp_path, path)


def _pick_latest_existing_path(*paths):
    candidates = [p for p in paths if p and os.path.exists(p)]
    if not candidates:
        return None
    return max(candidates, key=os.path.getmtime)


def _load_software_payload():
    source_path = _pick_latest_existing_path(SOFTWARE_FILE, SOFTWARE_LEGACY_FILE)
    if source_path:
        try:
            payload = parse_software_comps(source_path)
            _write_json_file(SOFTWARE_CACHE_FILE, payload)
            return payload
        except Exception:
            if os.path.exists(SOFTWARE_CACHE_FILE):
                return _load_json_file(SOFTWARE_CACHE_FILE)
            raise
    if os.path.exists(SOFTWARE_CACHE_FILE):
        return _load_json_file(SOFTWARE_CACHE_FILE)
    raise FileNotFoundError(
        f"Software screening data not found. Expected '{SOFTWARE_FILE}' "
        f"or legacy path '{SOFTWARE_LEGACY_FILE}' "
        f"or fallback cache '{SOFTWARE_CACHE_FILE}'."
    )


def _load_itservices_payload():
    source_path = _pick_latest_existing_path(ITSERVICES_FILE, ITSERVICES_LEGACY_FILE)
    if source_path:
        try:
            payload = parse_itservices_comps(source_path)
            _write_json_file(ITSERVICES_CACHE_FILE, payload)
            return payload
        except Exception:
            if os.path.exists(ITSERVICES_CACHE_FILE):
                return _load_json_file(ITSERVICES_CACHE_FILE)
            raise
    if os.path.exists(ITSERVICES_CACHE_FILE):
        return _load_json_file(ITSERVICES_CACHE_FILE)
    raise FileNotFoundError(
        f"IT Services screening data not found. Expected '{ITSERVICES_FILE}' "
        f"or legacy path '{ITSERVICES_LEGACY_FILE}' "
        f"or fallback cache '{ITSERVICES_CACHE_FILE}'."
    )


def _load_cached_last_updated():
    if os.path.exists(LAST_UPDATED_CACHE_FILE):
        payload = _load_json_file(LAST_UPDATED_CACHE_FILE)
        if isinstance(payload, dict):
            return payload.get("last_updated")

    cache_candidates = [p for p in (SOFTWARE_CACHE_FILE, ITSERVICES_CACHE_FILE) if os.path.exists(p)]
    if cache_candidates:
        latest = max(os.path.getmtime(p) for p in cache_candidates)
        return datetime.fromtimestamp(latest).strftime("%Y-%m-%d")

    return None


def _resolve_last_updated():
    source_path = _pick_latest_existing_path(
        SOFTWARE_FILE,
        SOFTWARE_LEGACY_FILE,
        ITSERVICES_FILE,
        ITSERVICES_LEGACY_FILE,
    )
    if source_path:
        ts = datetime.fromtimestamp(os.path.getmtime(source_path)).strftime("%Y-%m-%d")
        _write_json_file(LAST_UPDATED_CACHE_FILE, {"last_updated": ts})
        return ts

    return _load_cached_last_updated()


@app.route("/")
@app.route("/global")
def index():
    return render_template("index.html")


@app.route("/api/software")
def api_software():
    view = request.args.get("view", "gaap")
    try:
        data = _load_software_payload()
    except FileNotFoundError as e:
        return jsonify({"error": str(e)}), 503
    if view == "nongaap":
        return jsonify(data["nongaap"])
    return jsonify(data["gaap"])


@app.route("/api/itservices")
def api_itservices():
    try:
        data = _load_itservices_payload()
    except FileNotFoundError as e:
        return jsonify({"error": str(e)}), 503
    return jsonify(data)


@app.route("/api/last-updated")
def api_last_updated():
    ts = _resolve_last_updated()
    return jsonify({"last_updated": ts})


# ── Pairs APIs (public, read-only) ─────────────────────────────────────────

@app.route("/api/pairs")
def api_pairs():
    try:
        pairs = pairs_service.get_all_pairs()
        return jsonify(pairs)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/pairs/export")
def api_pairs_export():
    key = request.headers.get("X-Upload-Key") or request.args.get("key")
    if key != UPLOAD_KEY:
        return jsonify({"error": "Unauthorized"}), 403
    conn = pairs_service._get_db()
    rows = conn.execute("SELECT * FROM pairs ORDER BY sort_order ASC, id ASC").fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route("/api/pairs/<int:pair_id>/history")
def api_pair_history(pair_id):
    try:
        to_date = request.args.get("to") or datetime.utcnow().isoformat()
        from_date = request.args.get("from") or (datetime.utcnow() - timedelta(days=30)).isoformat()
        history = pairs_service.get_pair_history(pair_id, from_date, to_date)
        return jsonify(history)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Admin auth ─────────────────────────────────────────────────────────────

def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("admin"):
            return redirect(url_for("admin_login"))
        return f(*args, **kwargs)
    return decorated


@app.route("/admin/login", methods=["GET", "POST"])
def admin_login():
    error = None
    if request.method == "POST":
        if request.form.get("password") == UPLOAD_KEY:
            session["admin"] = True
            return redirect(request.args.get("next") or url_for("admin"))
        error = "Invalid password"
    return render_template("admin_login.html", error=error)


@app.route("/admin/logout")
def admin_logout():
    session.pop("admin", None)
    return redirect(url_for("index"))


# ── Admin pages ─────────────────────────────────────────────────────────────

@app.route("/admin")
@admin_required
def admin():
    return render_template("admin.html")


@app.route("/admin/pairs")
@admin_required
def admin_pairs():
    return render_template("admin_pairs.html")


# ── Admin APIs (pairs management — require login or upload key) ───────────

def _is_admin():
    """Check if request is from logged-in admin or has valid upload key."""
    if session.get("admin"):
        return True
    key = request.headers.get("X-Upload-Key") or request.form.get("key")
    return key == UPLOAD_KEY


@app.route("/api/pairs", methods=["POST"])
def api_create_pair():
    if not _is_admin():
        return jsonify({"error": "Unauthorized"}), 403
    try:
        data = request.get_json()
        pair = pairs_service.create_pair(data)
        return jsonify(pair), 201
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/pairs/<int:pair_id>", methods=["DELETE"])
def api_delete_pair(pair_id):
    if not _is_admin():
        return jsonify({"error": "Unauthorized"}), 403
    try:
        deleted = pairs_service.delete_pair(pair_id)
        if not deleted:
            return jsonify({"error": "Pair not found"}), 404
        return jsonify({"message": "Pair deleted"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/pairs/reorder", methods=["POST"])
def api_reorder_pairs():
    if not _is_admin():
        return jsonify({"error": "Unauthorized"}), 403
    try:
        data = request.get_json()
        ordered_ids = data.get("ordered_ids", [])
        pairs_service.reorder_pairs(ordered_ids)
        return jsonify({"message": "Reordered"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/pairs/<int:pair_id>/update-close", methods=["POST"])
def api_update_close(pair_id):
    if not _is_admin():
        return jsonify({"error": "Unauthorized"}), 403
    try:
        data = request.get_json()
        closed_date = data.get("closed_date") or None
        close_price_long = data.get("close_price_long") or None
        close_price_short = data.get("close_price_short") or None
        pair = pairs_service.update_pair_close(pair_id, closed_date, close_price_long, close_price_short)
        if not pair:
            return jsonify({"error": "Pair not found"}), 404
        return jsonify(pair)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Upload API ──────────────────────────────────────────────────────────────

@app.route("/api/upload", methods=["POST"])
def api_upload():
    if not _is_admin():
        return jsonify({"error": "Invalid upload key"}), 403

    results = []

    for field, filename in ALLOWED_FILES.items():
        file = request.files.get(field)
        if file and file.filename:
            dest = os.path.join(DATA_DIR, filename)
            file.save(dest)
            results.append(f"{field}: uploaded")

    if not results:
        return jsonify({"error": "No files provided"}), 400

    return jsonify({"status": "ok", "results": results})


# ── Anatel Upload + Reprocess ──────────────────────────────────────────────

@app.route("/api/upload-anatel", methods=["POST"])
def api_upload_anatel():
    if not _is_admin():
        return jsonify({"error": "Unauthorized"}), 403

    results = []

    for field, target_dir in [("broadband", process_data_telecom.BB_DIR), ("mobile", process_data_telecom.MOB_DIR)]:
        files = request.files.getlist(field)
        for file in files:
            if file and file.filename:
                fname = secure_filename(file.filename)
                os.makedirs(target_dir, exist_ok=True)
                file.save(os.path.join(target_dir, fname))
                results.append(f"{field}: {fname}")

    # Portability CSV (single file, saved to the exact path expected by ETL)
    port_file = request.files.get("portability")
    if port_file and port_file.filename:
        os.makedirs(os.path.dirname(process_data_telecom.PORT_CSV), exist_ok=True)
        port_file.save(process_data_telecom.PORT_CSV)
        results.append(f"portability: {port_file.filename}")

    # Direct DB upload (pre-built anatel.db)
    db_file = request.files.get("anatel_db")
    if db_file and db_file.filename:
        os.makedirs(os.path.dirname(process_data_telecom.DB_PATH), exist_ok=True)
        db_file.save(process_data_telecom.DB_PATH)
        results.append(f"anatel_db: {db_file.filename}")

    if not results:
        return jsonify({"error": "No files provided"}), 400

    # If we got a direct DB upload, skip reprocessing
    if any(r.startswith("anatel_db:") for r in results):
        return jsonify({"status": "ok", "results": results})

    try:
        has_bb_mob = any(r.startswith("broadband:") or r.startswith("mobile:") for r in results)
        has_port = any(r.startswith("portability:") for r in results)

        if has_bb_mob:
            bb = process_data_telecom.process_broadband()
            mob = process_data_telecom.process_mobile()
            port = process_data_telecom.process_portability()
            process_data_telecom.save_to_db(bb, mob, port)
            results.append("database: full rebuild")
        elif has_port:
            port = process_data_telecom.process_portability()
            if port is not None:
                process_data_telecom.save_portability_to_db(port)
                results.append("database: portability rebuilt")
    except Exception as e:
        return jsonify({"error": f"Upload ok but reprocess failed: {e}", "results": results}), 500

    return jsonify({"status": "ok", "results": results})


import atexit

@atexit.register
def _stop_monitors():
    for key in ("bz_monitor", "sec_monitor"):
        monitor = app.config.get(key)
        if monitor:
            try:
                monitor.stop()
            except Exception:
                pass


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(debug=True, host="0.0.0.0", port=port, use_reloader=False)
