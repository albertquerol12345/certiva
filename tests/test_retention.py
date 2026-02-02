from __future__ import annotations

import os
import time
from pathlib import Path

from src import utils


def test_delete_old_files(tmp_path):
    old_dir = tmp_path / "old"
    new_dir = tmp_path / "new"
    old_dir.mkdir()
    new_dir.mkdir()
    old_file = old_dir / "a.txt"
    new_file = new_dir / "b.txt"
    old_file.write_text("old")
    new_file.write_text("new")
    old_ts = time.time() - (5 * 24 * 3600)
    os.utime(old_file, (old_ts, old_ts))
    removed = utils.delete_old_files([tmp_path], max_age_days=2)
    assert removed == 1
    assert not old_file.exists()
    assert new_file.exists()
