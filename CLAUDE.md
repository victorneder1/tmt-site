# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Global AIM** — Flask web app for financial screening data visualization (Visible Alpha comparables) and trading pair performance tracking (Yahoo Finance). Deployed on Railway.

## Commands

```bash
# Run locally (Windows)
start.bat                    # Opens browser + starts Flask dev server on port 8080
python app.py                # Direct Flask launch

# Refresh data from Visible Alpha (Windows only, requires Excel + COM)
update_data.bat              # Runs update_excel.py → upload_to_server.py

# Production (Railway/Heroku)
gunicorn app:app --bind 0.0.0.0:$PORT

# Install dependencies
pip install -r requirements.txt              # Production
pip install -r requirements-local.txt        # Windows COM automation (pywin32)
```

## Architecture

### Data Flow

1. **Screening**: Excel files (Visible Alpha) → `data_parser.py` (dynamic column parsing with mtime cache) → `/api/software`, `/api/itservices` → `app.js` (table rendering with grouped headers, sorting, median)
2. **Pairs**: Admin creates pairs → `pairs_service.py` (SQLite CRUD + Yahoo Finance prices with 30s cache) → `/api/pairs` → `pairs.js` (cards + Chart.js graphs, auto-refresh 30s)
3. **Data Refresh**: `update_excel.py` (COM automation, Visible Alpha add-in) → `upload_to_server.py` (POST to `/api/upload` with UPLOAD_KEY)

### Key Files

| File | Role |
|------|------|
| `app.py` | Flask routes, admin auth (session + X-Upload-Key header), file upload |
| `data_parser.py` | Dynamic Excel parsing — reads row 2 (groups) + row 3 (sub-headers), infers column types, mtime-based cache |
| `pairs_service.py` | SQLite DB (`data/pairs.db`), Yahoo Finance price fetching (ThreadPoolExecutor, 8 workers), supports single tickers and basket pairs |
| `upload_to_server.py` | Script to POST Excel files to server with auth key |
| `update_excel.py` | Windows-only COM automation: kills Excel, registers XLL add-in, waits for async queries (max 300s), saves files |
| `static/app.js` | Screening UI: dynamic grouped headers, sort, search, GAAP/NonGAAP toggle, BTGe badges, auto-refresh 5min |
| `static/pairs.js` | Pairs UI: performance cards, Chart.js history graphs, inception/close date clamping, auto-refresh 30s |
| `telecom.py` | Flask Blueprint (`/telecom`), queries Anatel SQLite DB (`data/anatel.db`) for broadband & mobile operator data |
| `process_data_telecom.py` | ETL for Anatel data: aggregates operator market share from raw CSVs/Excel, handles old (pivoted CSV) and new (Excel) formats, normalizes operator names |
| `static/telecom/broadband.js`, `mobile.js` | Telecom dashboard UI: operator market share charts filtered by state/month/tech |

### Telecom Blueprint

Registered as `telecom_bp` with prefix `/telecom`. Has its own templates (`templates/telecom/`), styles (`static/telecom/style.css`), and JS files. Queries `data/anatel.db` (Brazilian Anatel regulatory data) with endpoints for broadband and mobile data, filterable by UF (state), month, technology, and segment.

### Authentication

- **Public**: `/`, `/api/software`, `/api/itservices`, `/api/last-updated`, `/api/pairs`, `/api/pairs/<id>/history`
- **Admin**: Session-based login (`/admin/login`) or `X-Upload-Key` header
- BTG estimate companies (highlighted with "BTGe" badge): defined in `BTG_COMPANIES` array in `app.js`

## Database Schemas

**pairs.db** — `pairs` table: `id` (PK), `long_ticker`, `short_ticker`, `entry_price_long`, `entry_price_short` (single value or JSON array for baskets), `entry_date`, `inception_date`, `status` (open/closed), `closed_date`, `close_price_long`, `close_price_short`, `sort_order`, `created_at`.

**anatel.db** — `broadband` table: `operator`, `month` (YYYY-MM), `UF`, `tech`, `accesses`. `mobile` table: `operator`, `month` (YYYY-MM), `UF`, `segment`, `accesses`.

## API Routes

Main: `/api/software`, `/api/itservices`, `/api/pairs`, `/api/pairs/<id>/history`, `/api/pairs/<id>` (DELETE), `/api/pairs/reorder`, `/api/pairs/<id>/update-close`, `/api/upload`, `/api/upload-anatel`, `/api/last-updated`.

Telecom: `/telecom/api/broadband`, `/telecom/api/broadband/months`, `/telecom/api/broadband/states`, `/telecom/api/mobile`, `/telecom/api/mobile/months`.

## Environment Variables

- `SECRET_KEY` — Flask session secret
- `UPLOAD_KEY` — API auth for file uploads
- `SERVER_URL` — Remote server URL (used by upload_to_server.py)
- `DATA_DIR` — Data directory path (default: app directory)
- `PORT` — Server port (default: 8080)

## Frontend

- Vanilla JS (no framework), Chart.js 4 via CDN for charts
- Global state in module-level variables (`pairsData`, `softwareData`, etc.)
- Auto-refresh intervals: 5min for screening data, 30s for pairs
- BTG Pactual branding: custom fonts in `static/fonts/`, dark navy header (#001F62)

## Conventions

- Excel column types are inferred from header text: "%" → percent, "x" suffix → multiple, otherwise number
- Column keys are auto-generated as snake_case from group+sub-header labels
- The `data_parser.py` forward-fills merged Excel group headers
- Data files (`.xlsx`, `.db`) and `.env` are gitignored — never commit them
- Pair trades support basket pairs (multiple tickers stored as JSON arrays in SQLite)
- Performance = avg(long returns) − avg(short returns), with inception/entry date distinction
- No ORM — raw SQL with `sqlite3.Row` factory throughout
- Portuguese is acceptable for user-facing communication (Brazilian team)
