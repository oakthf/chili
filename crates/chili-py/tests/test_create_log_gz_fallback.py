"""init_tick / createLog discovers <date>.gz when plain dated path is missing."""

from pathlib import Path

import gzip
import polars as pl
from chili import ChiliEngine


def test_create_log_falls_back_to_gz_suffix(tmp_path: Path):
    plain = tmp_path / "2026.07.22"
    raw = bytearray([255, 0, 0, 0, 0, 0, 0, 0])
    gz_path = tmp_path / "2026.07.22.gz"
    with gzip.open(gz_path, "wb") as f:
        f.write(raw)

    assert not plain.exists()
    assert gz_path.exists()

    e = ChiliEngine(pepper=True)
    schema = {"trade": pl.DataFrame({"sym": pl.Series([], dtype=pl.Utf8)})}
    e.init_tick(schema, str(tmp_path) + "/", "2026.07.22")
    msg_log = str(e.get_var(".tick.msgLog"))
    assert msg_log.endswith(".gz"), msg_log
    e.shutdown()
