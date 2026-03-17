from __future__ import annotations

import json
from pathlib import Path


class SeenDocumentsStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def load(self) -> set[str]:
        if not self.path.exists():
            return set()

        payload = json.loads(self.path.read_text(encoding="utf-8"))
        return set(payload.get("seen_protocols", []))

    def load_alerted(self) -> set[str]:
        if not self.path.exists():
            return set()

        payload = json.loads(self.path.read_text(encoding="utf-8"))
        return set(payload.get("alerted_protocols", []))

    def load_recent_documents(self) -> list[dict[str, str]]:
        if not self.path.exists():
            return []

        payload = json.loads(self.path.read_text(encoding="utf-8"))
        recent_documents = payload.get("recent_documents", [])
        if not isinstance(recent_documents, list):
            return []
        return recent_documents

    def save(self, protocols: set[str]) -> None:
        alerted_protocols = self.load_alerted()
        recent_documents = self.load_recent_documents()
        data = {
            "seen_protocols": sorted(protocols),
            "alerted_protocols": sorted(alerted_protocols),
            "recent_documents": recent_documents,
        }
        self.path.write_text(json.dumps(data, ensure_ascii=True, indent=2), encoding="utf-8")

    def save_state(
        self,
        protocols: set[str],
        alerted_protocols: set[str],
        recent_documents: list[dict[str, str]] | None = None,
    ) -> None:
        data = {
            "seen_protocols": sorted(protocols),
            "alerted_protocols": sorted(alerted_protocols),
            "recent_documents": recent_documents or self.load_recent_documents(),
        }
        self.path.write_text(json.dumps(data, ensure_ascii=True, indent=2), encoding="utf-8")
