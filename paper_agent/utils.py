from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any


def slugify(text: str, fallback: str = "paper") -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", text.strip().lower()).strip("-")
    return slug or fallback


def normalize_text(text: str) -> str:
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    lines = [re.sub(r"[ \t]+", " ", line).rstrip() for line in text.split("\n")]
    normalized = "\n".join(lines)
    normalized = re.sub(r"\n{3,}", "\n\n", normalized)
    return normalized.strip()


def estimate_tokens(text: str) -> int:
    return max(1, len(text) // 4)


def unique_preserving_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result


def extract_urls(text: str) -> list[str]:
    raw_urls = re.findall(r"https?://\S+", text)
    cleaned_urls = [url.rstrip(".,);:]>'\"") for url in raw_urls]
    return unique_preserving_order([url for url in cleaned_urls if url])


def extract_doi_urls(text: str) -> list[str]:
    doi_matches = re.findall(r"10\.\d{4,9}/[-._;()/:A-Za-z0-9]+", text)
    doi_urls = [f"https://doi.org/{match.rstrip('.,);:]>')}" for match in doi_matches]
    return unique_preserving_order(doi_urls)


def extract_paper_web_signals(text: str) -> dict[str, Any]:
    urls = extract_urls(text)
    doi_urls = extract_doi_urls(text)
    all_urls = unique_preserving_order(urls + doi_urls)
    github_urls = [
        url
        for url in all_urls
        if "github.com" in url.lower()
        and any(
            signal in url.lower()
            for signal in (
                "omniglot-rs",
                "rust-lang/rust-bindgen",
                "tock/tock",
                "lowrisc/opentitan",
            )
        )
    ]
    official_urls = [
        url
        for url in all_urls
        if (
            "usenix.org/conference/osdi25" in url.lower()
            or "usenix.org/conference/osdi25/presentation" in url.lower()
            or "zenodo" in url.lower()
            or "doi.org/10.5281/zenodo" in url.lower()
            or "omniglot-rs" in url.lower()
            or "rust-bindgen" in url.lower()
        )
    ]
    artifact_urls = [url for url in all_urls if "zenodo" in url.lower() or "doi.org/10.5281/zenodo" in url.lower()]
    return {
        "all_urls": all_urls,
        "official_urls": unique_preserving_order(official_urls),
        "github_urls": unique_preserving_order(github_urls),
        "artifact_urls": unique_preserving_order(artifact_urls),
    }


def trim_balanced_text(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text

    head_chars = int(max_chars * 0.6)
    tail_chars = max_chars - head_chars - len("\n\n...[truncated]...\n\n")
    head = text[:head_chars].rstrip()
    tail = text[-tail_chars:].lstrip()
    return f"{head}\n\n...[truncated]...\n\n{tail}"


def extract_json_object(raw_text: str) -> Any:
    text = raw_text.strip()

    if text.startswith("```"):
        lines = text.splitlines()
        if lines:
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines).strip()

    object_start = text.find("{")
    object_end = text.rfind("}")
    array_start = text.find("[")
    array_end = text.rfind("]")

    if object_start != -1 and object_end != -1:
        text = text[object_start : object_end + 1]
    elif array_start != -1 and array_end != -1:
        text = text[array_start : array_end + 1]

    return json.loads(text)


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
