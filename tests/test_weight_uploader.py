from __future__ import annotations

from datetime import date, time
from pathlib import Path
import sys
import textwrap

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from garmin_weight_uploader import (
    deduplicate_measurements,
    filter_new_entries,
    load_weight_rows,
    measurement_key,
    prune_state_keys,
)


@pytest.fixture()
def sample_csv(tmp_path: Path) -> Path:
    content = textwrap.dedent(
        """
        날짜,시간,몸무게,체지방률,총 체수분,골량,근육량,기본 대사율
        2024.01.01,07:30:00,70.5,15.2,60.1,3.2,30.5,1500
        2024.01.01,22:15:00,70.2,15.1,59.9,3.3,30.7,1510
        2024.01.02,08:00:00,70.4,15.0,60.0,3.1,30.2,1495
        """
    ).strip()

    csv_path = tmp_path / "weight.csv"
    csv_path.write_text(content, encoding="utf-8")
    return csv_path


def test_load_weight_rows_parses_korean_columns(sample_csv: Path) -> None:
    df = load_weight_rows(sample_csv)

    assert list(df.columns) == ["date", "time", "weight_kg"]
    assert len(df) == 3

    first = df.iloc[0]
    assert first["date"] == date(2024, 1, 1)
    assert first["time"] == time(7, 30)
    assert first["weight_kg"] == pytest.approx(70.5)

    last = df.iloc[-1]
    assert last["date"] == date(2024, 1, 2)
    assert last["time"] == time(8, 0)
    assert last["weight_kg"] == pytest.approx(70.4)


def test_load_weight_rows_parses_kst_datetime(tmp_path: Path) -> None:
    content = textwrap.dedent(
        """
        KST,체중,체지방률
        2025-09-18 07:45:00,70.1,15.0
        2025-09-18 21:10:30,70.3,15.2
        2025-09-19 06:55:10,69.8,14.9
        """
    ).strip()

    csv_path = tmp_path / "kst.csv"
    csv_path.write_text(content, encoding="utf-8")

    df = load_weight_rows(csv_path)

    assert df["date"].tolist() == [
        date(2025, 9, 18),
        date(2025, 9, 18),
        date(2025, 9, 19),
    ]
    assert df.iloc[1]["time"] == time(21, 10, 30)
    assert df.iloc[2]["weight_kg"] == pytest.approx(69.8)


def test_deduplicate_measurements_removes_identical_entries(sample_csv: Path) -> None:
    df = load_weight_rows(sample_csv)
    duplicated = pd.concat([df, df.iloc[[1]]], ignore_index=True)

    unique = deduplicate_measurements(duplicated)

    assert len(unique) == len(df)
    pd.testing.assert_frame_equal(unique, df)


def test_filter_new_entries_skips_previously_uploaded(sample_csv: Path) -> None:
    df = deduplicate_measurements(load_weight_rows(sample_csv))

    existing_key = measurement_key(
        df.iloc[0]["date"], df.iloc[0]["time"], df.iloc[0]["weight_kg"]
    )

    filtered, skipped = filter_new_entries(df, {existing_key})

    assert skipped == 1
    assert len(filtered) == len(df) - 1
    assert existing_key not in {
        measurement_key(row["date"], row["time"], row["weight_kg"])
        for _, row in filtered.iterrows()
    }


def test_prune_state_keys_limits_history() -> None:
    keys = [
        "2024-01-03T08:00:00|70.300",
        "2024-01-01T08:00:00|70.100",
        "2024-01-02T08:00:00|70.200",
    ]

    pruned = prune_state_keys(keys, limit=2)

    assert pruned == [
        "2024-01-02T08:00:00|70.200",
        "2024-01-03T08:00:00|70.300",
    ]

