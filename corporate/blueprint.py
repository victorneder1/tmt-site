from __future__ import annotations

from pathlib import Path
from typing import Any

from flask import Blueprint, current_app, jsonify, render_template

from .analytics import build_analytics_tables
from .config import load_corporate_config
from .monitor import CVMMonitor

BASE_DIR = Path(__file__).parent.parent  # tmt-site root

tracker_bp = Blueprint(
    "tracker",
    __name__,
    url_prefix="/tracker",
)


def create_tracker_bp() -> Blueprint:
    bz_config = load_corporate_config(BASE_DIR)
    bz_monitor = CVMMonitor(bz_config)

    @tracker_bp.record_once
    def _startup(state: Any) -> None:
        state.app.config["bz_monitor"] = bz_monitor
        bz_monitor.start()

    @tracker_bp.get("/")
    def index():
        return render_template("corporate/index.html")

    @tracker_bp.get("/api/status")
    def api_status():
        bz = current_app.config.get("bz_monitor")
        bz_snapshot = bz.get_snapshot() if bz else {}
        movements = bz_snapshot.get("movements", [])
        companies = [{**c, "market": "BZ"} for c in bz_snapshot.get("companies", [])]

        return jsonify({
            "companies": companies,
            "analytics": build_analytics_tables(movements),
            "history_documents": bz_snapshot.get("history_documents", []),
            "last_success_at": bz_snapshot.get("last_success_at"),
            "last_error": bz_snapshot.get("last_error"),
        })

    @tracker_bp.post("/api/refresh")
    def api_refresh():
        bz = current_app.config.get("bz_monitor")
        if bz:
            bz.force_refresh()
        return api_status()

    return tracker_bp
