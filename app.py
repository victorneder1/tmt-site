import os
from datetime import datetime, timedelta
from flask import Flask, render_template, jsonify, request, redirect, url_for
from werkzeug.utils import secure_filename

from data_parser import parse_software_comps, parse_itservices_comps, get_last_updated
import pairs_service

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("DATA_DIR", BASE_DIR)
os.makedirs(DATA_DIR, exist_ok=True)
SOFTWARE_FILE = os.path.join(DATA_DIR, "Screening_VisibleAlpha_Software_site.xlsx")
ITSERVICES_FILE = os.path.join(DATA_DIR, "Screening_VisibleAlpha_ITServices_site.xlsx")

# Upload key for authentication — change this to a strong secret before deploying
UPLOAD_KEY = os.environ.get("UPLOAD_KEY", "change-me-before-deploy")

ALLOWED_FILES = {
    "software": os.path.basename(SOFTWARE_FILE),
    "itservices": os.path.basename(ITSERVICES_FILE),
}


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/software")
def api_software():
    view = request.args.get("view", "gaap")
    data = parse_software_comps(SOFTWARE_FILE)
    if view == "nongaap":
        return jsonify(data["nongaap"])
    return jsonify(data["gaap"])


@app.route("/api/itservices")
def api_itservices():
    data = parse_itservices_comps(ITSERVICES_FILE)
    return jsonify(data)


@app.route("/api/last-updated")
def api_last_updated():
    ts = get_last_updated(SOFTWARE_FILE, ITSERVICES_FILE)
    return jsonify({"last_updated": ts})


# ── Pairs APIs (public, read-only) ─────────────────────────────────────────

@app.route("/api/pairs")
def api_pairs():
    try:
        pairs = pairs_service.get_all_pairs()
        return jsonify(pairs)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/pairs/<int:pair_id>/history")
def api_pair_history(pair_id):
    try:
        to_date = request.args.get("to") or datetime.utcnow().isoformat()
        from_date = request.args.get("from") or (datetime.utcnow() - timedelta(days=30)).isoformat()
        history = pairs_service.get_pair_history(pair_id, from_date, to_date)
        return jsonify(history)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Admin pages ─────────────────────────────────────────────────────────────

@app.route("/admin")
def admin():
    return render_template("admin.html")


@app.route("/admin/pairs")
def admin_pairs():
    return render_template("admin_pairs.html")


# ── Admin APIs (pairs management) ──────────────────────────────────────────

@app.route("/api/pairs", methods=["POST"])
def api_create_pair():
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
    try:
        deleted = pairs_service.delete_pair(pair_id)
        if not deleted:
            return jsonify({"error": "Pair not found"}), 404
        return jsonify({"message": "Pair deleted"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/pairs/reorder", methods=["POST"])
def api_reorder_pairs():
    try:
        data = request.get_json()
        ordered_ids = data.get("ordered_ids", [])
        pairs_service.reorder_pairs(ordered_ids)
        return jsonify({"message": "Reordered"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/pairs/<int:pair_id>/update-close", methods=["POST"])
def api_update_close(pair_id):
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
    key = request.form.get("key") or request.headers.get("X-Upload-Key")
    if key != UPLOAD_KEY:
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


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(debug=True, host="0.0.0.0", port=port)
