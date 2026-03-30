from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from paper_agent.utils import slugify


@dataclass
class ConferencePaper:
    venue: str
    year: int
    title: str
    authors: list[str] = field(default_factory=list)
    session: str | None = None
    detail_url: str | None = None
    pdf_url: str | None = None
    preprint_url: str | None = None
    doi_url: str | None = None
    landing_page_url: str | None = None
    discovery_source: str | None = None
    source_urls: list[str] = field(default_factory=list)
    alternate_urls: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    resolution_trace: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    status: str = "discovered"
    download_url: str | None = None
    download_path: str | None = None
    download_error: str | None = None

    def paper_id(self) -> str:
        return f"{self.venue}-{self.year}-{slugify(self.title, fallback='paper')}"

    def add_source_url(self, url: str | None) -> None:
        normalized = str(url or "").strip()
        if normalized and normalized not in self.source_urls:
            self.source_urls.append(normalized)

    def add_note(self, note: str | None) -> None:
        normalized = str(note or "").strip()
        if normalized and normalized not in self.notes:
            self.notes.append(normalized)

    def add_alternate_url(self, url: str | None) -> None:
        normalized = str(url or "").strip()
        if normalized and normalized not in self.alternate_urls:
            self.alternate_urls.append(normalized)
            self.add_source_url(normalized)

    def add_trace(self, message: str | None) -> None:
        normalized = str(message or "").strip()
        if normalized:
            self.resolution_trace.append(normalized)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ConferenceManifest:
    venue: str
    year: int
    index_url: str
    generated_at: str
    status: str
    items: list[ConferencePaper] = field(default_factory=list)
    manifest_path: str | None = None
    unresolved_path: str | None = None
    error: str | None = None
    warnings: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["paper_count"] = len(self.items)
        payload["downloaded_count"] = sum(1 for item in self.items if item.status in {"downloaded", "existing"})
        payload["unresolved_count"] = sum(1 for item in self.items if item.status == "unresolved")
        payload["pending_count"] = sum(1 for item in self.items if item.status not in {"downloaded", "existing", "unresolved"})
        return payload
