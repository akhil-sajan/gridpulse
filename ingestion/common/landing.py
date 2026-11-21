"""Building landing-zone paths and writing raw files to them.

Every raw file lands at::

    /Volumes/<catalog>/bronze/landing/<source>/<entity>/ingest_date=YYYY-MM-DD/<filename>

Two writers share one interface:

- ``VolumeLandingWriter`` writes to the Unity Catalog volume through the Databricks
  Files API (backed by our S3 bucket).
- ``LocalLandingWriter`` mirrors the same paths into a local folder, for tests and
  offline development.

``upload_to_landing`` skips files that already exist, so re-running an ingester is safe.
"""

from __future__ import annotations

import io
import logging
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Protocol

from ingestion.common.config import Settings
from ingestion.common.logs import get_logger

_NAME_RE = re.compile(r"^[a-z0-9_]+$")
_FILENAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._=-]*$")

log = get_logger(__name__)


def build_landing_path(
    root: str,
    source: str,
    entity: str,
    filename: str,
    ingest_date: date | None = None,
) -> str:
    """Return the full landing path for one file.

    ``source`` and ``entity`` must be lowercase snake_case; ``filename`` must not contain
    slashes. ``ingest_date`` defaults to today in UTC.
    """
    for label, value in (("source", source), ("entity", entity)):
        if not _NAME_RE.fullmatch(value):
            raise ValueError(f"{label} must be lowercase letters, digits or _: {value!r}")
    if not _FILENAME_RE.fullmatch(filename) or ".." in filename:
        raise ValueError(f"invalid filename: {filename!r}")
    day = ingest_date or datetime.now(UTC).date()
    return f"{root.rstrip('/')}/{source}/{entity}/ingest_date={day.isoformat()}/{filename}"


class LandingWriter(Protocol):
    def exists(self, path: str) -> bool: ...

    def write(self, path: str, data: bytes, overwrite: bool = False) -> None: ...

    def list_files(self, directory: str) -> list[str]:
        """All file paths under ``directory`` (recursive); empty if it doesn't exist."""
        ...


class LocalLandingWriter:
    """Writes ``/Volumes/...`` paths under a local base folder."""

    def __init__(self, base_dir: str | Path) -> None:
        self.base_dir = Path(base_dir)

    def _local(self, path: str) -> Path:
        return self.base_dir / path.lstrip("/")

    def exists(self, path: str) -> bool:
        return self._local(path).is_file()

    def write(self, path: str, data: bytes, overwrite: bool = False) -> None:
        target = self._local(path)
        if target.exists() and not overwrite:
            raise FileExistsError(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)

    def list_files(self, directory: str) -> list[str]:
        base = self._local(directory)
        if not base.is_dir():
            return []
        return sorted(
            "/" + p.relative_to(self.base_dir).as_posix() for p in base.rglob("*") if p.is_file()
        )


class VolumeLandingWriter:
    """Writes to a Unity Catalog volume through the Databricks Files API."""

    def __init__(self, client: Any | None = None) -> None:
        # On Free Edition the SDK's attempt to use presigned upload URLs returns 403 and it
        # falls back to the standard Files API. Uploads work; hide the noisy warning.
        logging.getLogger("databricks.sdk.mixins.files").setLevel(logging.ERROR)
        if client is None:
            from databricks.sdk import WorkspaceClient

            client = WorkspaceClient()
        self.client = client

    def exists(self, path: str) -> bool:
        from databricks.sdk.errors import NotFound

        try:
            self.client.files.get_metadata(path)
        except NotFound:
            return False
        return True

    def write(self, path: str, data: bytes, overwrite: bool = False) -> None:
        self.client.files.upload(path, io.BytesIO(data), overwrite=overwrite)

    def list_files(self, directory: str) -> list[str]:
        from databricks.sdk.errors import NotFound

        found: list[str] = []
        pending = [directory.rstrip("/")]
        while pending:
            current = pending.pop()
            try:
                entries = list(self.client.files.list_directory_contents(current))
            except NotFound:
                continue
            for entry in entries:
                if entry.is_directory:
                    pending.append(entry.path.rstrip("/"))
                else:
                    found.append(entry.path)
        return sorted(found)


def make_writer(settings: Settings) -> LandingWriter:
    """Pick the writer configured by ``GRIDPULSE_LANDING_BACKEND``."""
    if settings.landing_backend == "local":
        return LocalLandingWriter(settings.local_landing_dir)
    return VolumeLandingWriter()


@dataclass(frozen=True)
class UploadResult:
    path: str
    skipped: bool
    bytes: int


def upload_to_landing(
    writer: LandingWriter,
    settings: Settings,
    source: str,
    entity: str,
    filename: str,
    data: bytes,
    ingest_date: date | None = None,
    overwrite: bool = False,
) -> UploadResult:
    """Write ``data`` to its landing path; skip it if the file is already there."""
    path = build_landing_path(settings.landing_root, source, entity, filename, ingest_date)
    if not overwrite and writer.exists(path):
        log.info("file already landed, skipping", extra={"path": path})
        return UploadResult(path=path, skipped=True, bytes=0)
    writer.write(path, data, overwrite=overwrite)
    log.info("file landed", extra={"path": path, "bytes": len(data)})
    return UploadResult(path=path, skipped=False, bytes=len(data))
