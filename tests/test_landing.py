from datetime import date

import pytest
from databricks.sdk.errors import NotFound

from ingestion.common.config import load_settings
from ingestion.common.landing import (
    LocalLandingWriter,
    VolumeLandingWriter,
    build_landing_path,
    make_writer,
    upload_to_landing,
)

ROOT = "/Volumes/gridpulse_dev/bronze/landing"
DAY = date(2026, 11, 21)


def test_builds_partitioned_path():
    path = build_landing_path(ROOT, "smard", "generation", "2026-11.json", DAY)
    assert path == f"{ROOT}/smard/generation/ingest_date=2026-11-21/2026-11.json"


def test_trailing_slash_on_root_is_ignored():
    path = build_landing_path(ROOT + "/", "smard", "prices", "a.csv", DAY)
    assert path.startswith(f"{ROOT}/smard/")
    assert "//" not in path


def test_default_ingest_date_is_today_utc():
    path = build_landing_path(ROOT, "smard", "prices", "a.csv")
    assert "/ingest_date=" in path


@pytest.mark.parametrize("bad", ["Smard", "smard-de", "smard/de", "", "smärd"])
def test_rejects_bad_source_names(bad):
    with pytest.raises(ValueError, match="source"):
        build_landing_path(ROOT, bad, "prices", "a.csv", DAY)


@pytest.mark.parametrize("bad", ["../x.csv", "a/b.csv", ".hidden", "", "a b.csv", "a..b.csv"])
def test_rejects_bad_filenames(bad):
    with pytest.raises(ValueError, match="filename"):
        build_landing_path(ROOT, "smard", "prices", bad, DAY)


def test_local_writer_mirrors_volume_paths(tmp_path):
    writer = LocalLandingWriter(tmp_path)
    path = f"{ROOT}/x/y/ingest_date=2026-11-21/f.json"
    assert not writer.exists(path)
    writer.write(path, b"{}")
    assert writer.exists(path)
    assert (tmp_path / path.lstrip("/")).read_bytes() == b"{}"
    with pytest.raises(FileExistsError):
        writer.write(path, b"again")
    writer.write(path, b"again", overwrite=True)
    assert (tmp_path / path.lstrip("/")).read_bytes() == b"again"


@pytest.fixture
def local_settings(tmp_path):
    return load_settings(
        environ={"GRIDPULSE_LANDING_BACKEND": "local", "GRIDPULSE_LOCAL_LANDING_DIR": str(tmp_path)}
    )


def test_upload_is_idempotent(local_settings):
    writer = make_writer(local_settings)
    assert isinstance(writer, LocalLandingWriter)

    first = upload_to_landing(writer, local_settings, "smard", "prices", "a.json", b"[1]", DAY)
    second = upload_to_landing(writer, local_settings, "smard", "prices", "a.json", b"[2]", DAY)

    assert first.skipped is False and first.bytes == 3
    assert second.skipped is True and second.bytes == 0
    assert second.path == first.path
    assert writer._local(first.path).read_bytes() == b"[1]"  # original kept


def test_upload_overwrite_replaces_file(local_settings):
    writer = make_writer(local_settings)
    upload_to_landing(writer, local_settings, "smard", "prices", "a.json", b"[1]", DAY)
    result = upload_to_landing(
        writer, local_settings, "smard", "prices", "a.json", b"[2]", DAY, overwrite=True
    )
    assert result.skipped is False
    assert writer._local(result.path).read_bytes() == b"[2]"


class FakeFilesApi:
    def __init__(self):
        self.store = {}

    def get_metadata(self, path):
        if path not in self.store:
            raise NotFound(f"{path} not found")
        return object()

    def upload(self, path, contents, overwrite=False):
        if path in self.store and not overwrite:
            raise FileExistsError(path)
        self.store[path] = contents.read()


class FakeWorkspaceClient:
    def __init__(self):
        self.files = FakeFilesApi()


def test_volume_writer_uses_files_api():
    client = FakeWorkspaceClient()
    writer = VolumeLandingWriter(client=client)
    path = f"{ROOT}/x/y/ingest_date=2026-11-21/f.json"

    assert writer.exists(path) is False
    writer.write(path, b"data")
    assert writer.exists(path) is True
    assert client.files.store[path] == b"data"
