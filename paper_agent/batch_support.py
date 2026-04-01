from __future__ import annotations

import json
import shutil
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from paper_agent.config import RuntimeConfig
from paper_agent.utils import build_collected_pdf_name, slugify, write_json


@dataclass(slots=True)
class BatchJob:
    pdf_path: str
    run_dir: str
    collected_pdf_path: str
    legacy_collected_pdf_path: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def iter_pdf_paths(input_dir: Path, recursive: bool) -> list[Path]:
    iterator = input_dir.rglob("*") if recursive else input_dir.iterdir()
    pdfs = [path.resolve() for path in iterator if path.is_file() and path.suffix.lower() == ".pdf"]
    return sorted(pdfs, key=lambda path: path.name.lower())


def resolve_collect_dir(input_dir: Path, collect_dir: str | None) -> Path:
    if collect_dir:
        return Path(collect_dir).expanduser().resolve()
    return (input_dir / "paper-agent-final-pdfs").resolve()


def resolve_batch_root(config: RuntimeConfig, input_dir: Path, output_dir: str | None) -> Path:
    if output_dir:
        return Path(output_dir).expanduser().resolve()
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    return (config.output_root / f"{timestamp}-batch-{slugify(input_dir.name or 'papers')}").resolve()


def write_batch_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def make_job_run_dir(batch_root: Path, pdf_path: Path, used_names: dict[str, int]) -> Path:
    base = slugify(pdf_path.stem, fallback="paper")
    count = used_names.get(base, 0) + 1
    used_names[base] = count
    dir_name = base if count == 1 else f"{base}-{count}"
    return batch_root / dir_name


def build_batch_jobs(batch_root: Path, pdf_paths: list[Path], collector: "BatchCollector") -> list[BatchJob]:
    used_run_dir_names: dict[str, int] = {}
    jobs: list[BatchJob] = []
    for pdf_path in pdf_paths:
        run_dir = make_job_run_dir(batch_root, pdf_path, used_run_dir_names)
        jobs.append(collector.build_job(pdf_path, run_dir))

    legacy_counts = Counter(job.legacy_collected_pdf_path for job in jobs if job.legacy_collected_pdf_path)
    for job in jobs:
        if job.collected_pdf_path:
            continue
        if legacy_counts.get(job.legacy_collected_pdf_path, 0) > 1:
            job.legacy_collected_pdf_path = ""
    return jobs


def partition_batch_jobs(jobs: list[BatchJob], skip_existing: bool) -> tuple[list[dict[str, Any]], list[BatchJob]]:
    if not skip_existing:
        return [], jobs

    completed: list[dict[str, Any]] = []
    pending: list[BatchJob] = []
    for job in jobs:
        existing_output = resolve_existing_job_output(job)
        if existing_output is None:
            pending.append(job)
            continue
        completed.append(
            {
                **job.to_dict(),
                "status": "skipped_existing",
                "collected_pdf_path": str(existing_output),
            }
        )
    return completed, pending


def resolve_existing_job_output(job: BatchJob) -> Path | None:
    indexed_path = Path(job.collected_pdf_path) if job.collected_pdf_path else None
    legacy_path = Path(job.legacy_collected_pdf_path) if job.legacy_collected_pdf_path else None
    if indexed_path is not None and indexed_path.exists():
        return indexed_path
    if legacy_path is not None and legacy_path.exists():
        return legacy_path
    return None


def _unique_output_name(base_name: str, used_names: set[str]) -> str:
    if base_name not in used_names:
        used_names.add(base_name)
        return base_name

    stem = Path(base_name).stem
    suffix = Path(base_name).suffix
    counter = 2
    while True:
        candidate = f"{stem}-{counter}{suffix}"
        if candidate not in used_names:
            used_names.add(candidate)
            return candidate
        counter += 1


def _collection_index_path(collect_dir: Path) -> Path:
    return collect_dir / "collection_index.json"


def _load_collection_index(collect_dir: Path) -> dict[str, dict[str, Any]]:
    index_path = _collection_index_path(collect_dir)
    if not index_path.exists():
        return {}
    try:
        payload = json.loads(index_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    return {
        str(source_pdf_path): value
        for source_pdf_path, value in payload.items()
        if isinstance(value, dict)
    }


class BatchCollector:
    def __init__(self, collect_dir: Path) -> None:
        self.collect_dir = collect_dir.expanduser().resolve()
        self.collect_dir.mkdir(parents=True, exist_ok=True)
        self.used_names = {path.name for path in self.collect_dir.glob("*.pdf")}
        self.collection_index = _load_collection_index(self.collect_dir)

    def build_job(self, pdf_path: Path, run_dir: Path) -> BatchJob:
        resolved_pdf_path = str(pdf_path.resolve())
        indexed_collected_path = str((self.collection_index.get(resolved_pdf_path) or {}).get("collected_pdf_path") or "")
        legacy_collected_path = str((self.collect_dir / f"{pdf_path.stem}.paper_agent.pdf").resolve())
        return BatchJob(
            pdf_path=resolved_pdf_path,
            run_dir=str(run_dir),
            collected_pdf_path=indexed_collected_path,
            legacy_collected_pdf_path=legacy_collected_path,
        )

    def collect_report_pdf(
        self,
        report_pdf_path: str | None,
        source_pdf_path: str,
        report_title: str | None,
    ) -> str | None:
        if not report_pdf_path:
            return None

        source_pdf_key = str(Path(source_pdf_path).resolve())
        previous_entry = self.collection_index.get(source_pdf_key) or {}
        previous_path_value = previous_entry.get("collected_pdf_path")
        previous_path = Path(previous_path_value).resolve() if previous_path_value else None
        previous_name = previous_path.name if previous_path and previous_path.parent == self.collect_dir else None

        if previous_name in self.used_names:
            self.used_names.remove(previous_name)

        output_name = _unique_output_name(build_collected_pdf_name(report_title, source_pdf_path), self.used_names)
        destination = self.collect_dir / output_name
        shutil.copy2(report_pdf_path, destination)

        if previous_path and previous_path.exists() and previous_path != destination and previous_path.parent == self.collect_dir:
            previous_path.unlink()

        self.collection_index[source_pdf_key] = {
            "paper_title": report_title or "",
            "collected_pdf_path": str(destination),
        }
        self.save()
        return str(destination)

    def save(self) -> None:
        write_json(_collection_index_path(self.collect_dir), self.collection_index)
