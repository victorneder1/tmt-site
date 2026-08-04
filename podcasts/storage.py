import json
import os
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
REPO_PODCASTS_DIR = BASE_DIR / "podcasts"
DEFAULT_DATA_DIR = Path("/data") if Path("/data").is_dir() else BASE_DIR / "data"
DATA_DIR = Path(os.environ.get("DATA_DIR", DEFAULT_DATA_DIR))

SUBMISSIONS_FILENAME = "submissions.json"
JOURNAL_FILENAME = "submissions.journal.jsonl"

PODCAST_OPTIONS = [
    "All-In Podcast",
    "Invest Like The Best",
    "Dwarkesh Patel",
    "The MAD Podcast",
    "SemiAnalysis Weekly",
    "The Circuit",
    "Sharp Tech Podcast",
    "Market Makers",
    "Stock Pickers",
]


def _load_json_file(path):
    with open(path, encoding="utf-8-sig") as f:
        return json.load(f)


def _write_json_file(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f"{path.name}.tmp")
    with open(temp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(temp_path, path)


def _is_writable_dir(path):
    try:
        path.mkdir(parents=True, exist_ok=True)
        test_path = path / ".write-test.tmp"
        test_path.write_text("ok", encoding="utf-8")
        test_path.unlink()
        return True
    except Exception:
        return False


def _storage_dirs():
    candidates = [
        DATA_DIR / "podcasts",
        Path("/data") / "podcasts",
        REPO_PODCASTS_DIR,
    ]
    dirs = []
    seen = set()
    for path in candidates:
        path = path.resolve()
        key = os.path.normcase(str(path))
        if key in seen:
            continue
        seen.add(key)
        if _is_writable_dir(path):
            dirs.append(path)
    return dirs


def _snapshot_paths():
    return [path / SUBMISSIONS_FILENAME for path in _storage_dirs()]


def _journal_paths():
    return [path / JOURNAL_FILENAME for path in _storage_dirs()]


def _normalize_email(email):
    return str(email or "").strip().lower()


def _normalize_record(record):
    if not isinstance(record, dict):
        return None

    name = str(record.get("name") or "").strip()
    email = str(record.get("email") or "").strip()
    podcasts = record.get("podcasts")
    updated_at = str(record.get("updated_at") or "").strip()

    if not name or not _normalize_email(email) or not isinstance(podcasts, list):
        return None

    cleaned_podcasts = []
    for podcast in podcasts:
        podcast = str(podcast or "").strip()
        if podcast in PODCAST_OPTIONS and podcast not in cleaned_podcasts:
            cleaned_podcasts.append(podcast)

    if not cleaned_podcasts:
        return None

    return {
        "name": name,
        "email": email,
        "podcasts": cleaned_podcasts,
        "updated_at": updated_at or datetime.utcnow().isoformat() + "Z",
    }


def _merge_records(records):
    by_email = {}
    for record in records:
        normalized = _normalize_record(record)
        if not normalized:
            continue
        key = _normalize_email(normalized["email"])
        current = by_email.get(key)
        if not current or normalized["updated_at"] >= current.get("updated_at", ""):
            by_email[key] = normalized
    return sorted(by_email.values(), key=lambda item: str(item.get("name", "")).lower())


def load_submissions():
    records = []

    for path in _snapshot_paths():
        if not path.exists():
            continue
        try:
            payload = _load_json_file(path)
            if isinstance(payload, list):
                records.extend(payload)
        except Exception:
            continue

    for path in _journal_paths():
        if not path.exists():
            continue
        try:
            with open(path, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        records.append(json.loads(line))
        except Exception:
            continue

    return _merge_records(records)


def validate_payload(payload):
    if not isinstance(payload, dict):
        raise ValueError("Invalid payload")

    name = str(payload.get("name") or "").strip()
    email = str(payload.get("email") or "").strip()
    email_key = _normalize_email(email)
    podcasts = payload.get("podcasts")

    if not name:
        raise ValueError("Name is required")
    if not email_key or "@" not in email_key:
        raise ValueError("Valid email is required")
    if not isinstance(podcasts, list):
        raise ValueError("Podcasts must be a list")

    cleaned_podcasts = []
    for podcast in podcasts:
        podcast = str(podcast or "").strip()
        if podcast in PODCAST_OPTIONS and podcast not in cleaned_podcasts:
            cleaned_podcasts.append(podcast)

    if not cleaned_podcasts:
        raise ValueError("Select at least one podcast")

    return {
        "name": name,
        "email": email,
        "podcasts": cleaned_podcasts,
        "updated_at": datetime.utcnow().isoformat() + "Z",
    }


def upsert_submission(submission):
    submissions = load_submissions()
    email = _normalize_email(submission["email"])
    replaced = False
    for idx, existing in enumerate(submissions):
        if _normalize_email(existing.get("email")) == email:
            submissions[idx] = submission
            replaced = True
            break
    if not replaced:
        submissions.append(submission)
    submissions.sort(key=lambda item: str(item.get("name", "")).lower())

    write_errors = []
    for path in _journal_paths():
        try:
            with open(path, "a", encoding="utf-8") as f:
                f.write(json.dumps(submission, ensure_ascii=False) + "\n")
        except Exception as e:
            write_errors.append(f"{path}: {e}")

    snapshot_written = False
    for path in _snapshot_paths():
        try:
            _write_json_file(path, submissions)
            snapshot_written = True
        except Exception as e:
            write_errors.append(f"{path}: {e}")

    if not snapshot_written:
        raise RuntimeError("; ".join(write_errors) or "Unable to save podcast submission")

    return submission
