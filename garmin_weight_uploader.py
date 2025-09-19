#!/usr/bin/env python3
"""Google Fit → Garmin Connect weight synchronizer.

This helper script imports body weight measurements exported from Google Fit
and forwards them to Garmin Connect.  It is designed to work well with a
Google Drive automation where new CSV files are periodically uploaded to a
folder on disk.

Key features
------------
* Flexible column detection that understands both Korean and English Google
  Fit exports.
* Flexible detection of timestamp columns, including combined ``KST`` values
  exported by Google Fit.
* Duplicate protection via a small JSON state file to avoid re-uploading the
  same measurement.
* Rich command-line interface with dry-run mode, timezone selection and
  explicit CSV selection.

Required environment variables
------------------------------
GARMIN_EMAIL / GARMIN_PASSWORD  Credentials for Garmin Connect.

Optional environment variables
------------------------------
GARMINTOKENS  Directory where authentication tokens are stored.  Defaults to
              ``~/.garminconnect``.

Example
-------
    $ export GARMIN_EMAIL="you@example.com"
    $ export GARMIN_PASSWORD="secret"
    $ python garmin_weight_uploader.py --timezone Asia/Seoul
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
from datetime import date as dt_date
from datetime import datetime, time as dt_time, timezone
from pathlib import Path
from typing import Iterable, Optional, Sequence, TYPE_CHECKING
from zoneinfo import ZoneInfo

import pandas as pd
from dateutil import parser as dateparser

if TYPE_CHECKING:  # pragma: no cover - only for static type checking
    from garminconnect import Garmin

# Default token directory respects the same environment variable as the
# library itself so that scripted runs and the interactive examples can share
# cached credentials.
TOKEN_DIR = Path(os.getenv("GARMINTOKENS", "~/.garminconnect")).expanduser()
DEFAULT_STATE_PATH = TOKEN_DIR / "weight_uploader_state.json"
DEFAULT_PATTERNS = ["*Google Fit*.csv", "*.csv"]
CSV_ENCODING = "utf-8-sig"

# Maximum number of historical entries to keep in the state file.  This is
# purely defensive; with one weigh-in per day the default keeps more than a
# year of history while keeping the JSON file tiny.
STATE_VERSION = 1
STATE_MAX_ENTRIES = 512

# Columns that we can recognise in the Google Fit export.  Keys map to the
# canonical field names understood by the Garmin API; the values are a set of
# known aliases.  Each alias is normalised (lowercase, stripped of whitespace
# and punctuation) before matching.
COLUMN_ALIASES: dict[str, set[str]] = {
    "date": {"날짜", "date"},
    "time": {"시간", "time"},
    "datetime": {"datetime", "timestamp", "kst", "timekst", "측정일시", "측정시간"},
    "weight": {"몸무게", "weight", "weightkg", "체중", "bodyweight"},
}


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def _normalise_column_name(name: str) -> str:
    """Return a simplified column identifier for matching."""

    return "".join(ch for ch in str(name).lower() if ch.isalnum())


def _find_column(df: pd.DataFrame, field: str) -> Optional[str]:
    """Return the DataFrame column matching the canonical ``field``."""

    aliases = COLUMN_ALIASES.get(field, set())
    if not aliases:
        return None

    lookup = { _normalise_column_name(col): col for col in df.columns }
    for alias in aliases:
        normalised = _normalise_column_name(alias)
        if normalised in lookup:
            return lookup[normalised]
    return None


def _parse_float(value: object) -> Optional[float]:
    """Parse ``value`` to ``float``; return ``None`` on failure."""

    if value is None:
        return None

    if isinstance(value, (int, float)):
        if isinstance(value, bool) or math.isnan(float(value)):
            return None
        return float(value)

    text = str(value).strip()
    if not text:
        return None

    if text in {"-", "--", "nan"}:
        return None

    # Replace non-breaking spaces and normalise decimal separators.  When the
    # string uses a single comma as decimal separator we replace it with a dot;
    # otherwise commas are treated as thousands separators and stripped.
    text = text.replace("\u00a0", " ").strip()
    if text.count(",") == 1 and "." not in text:
        text = text.replace(",", ".")
    else:
        text = text.replace(",", "")

    cleaned = (
        text.replace("kg", "")
        .replace("KG", "")
        .replace("kcal", "")
        .replace("KCAL", "")
        .replace("%", "")
    )
    cleaned = re.sub(r"[^0-9+\-\.Ee]", "", cleaned)
    if not cleaned or cleaned in {".", "-", "+", "-.", "+."}:
        return None

    try:
        return float(cleaned)
    except ValueError:
        return None


def _parse_datetime(value: object) -> Optional[datetime]:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None

    text = str(value).strip()
    if not text:
        return None

    try:
        return dateparser.parse(text, dayfirst=False, yearfirst=True, fuzzy=True)
    except (TypeError, ValueError, OverflowError):
        return None


def _parse_date(value: object) -> Optional[dt_date]:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None

    text = str(value).strip()
    if not text:
        return None

    try:
        dt = dateparser.parse(text, dayfirst=False, yearfirst=True, fuzzy=True)
    except (TypeError, ValueError, OverflowError):
        return None

    if dt is None:
        return None
    return dt.date()


def _parse_time(value: object) -> dt_time:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return dt_time(0, 0, 0)

    text = str(value).strip()
    if not text:
        return dt_time(0, 0, 0)

    try:
        dt = dateparser.parse(text, default=datetime(1970, 1, 1), fuzzy=True)
    except (TypeError, ValueError):
        return dt_time(0, 0, 0)

    return dt.time()


def measurement_key(date_value: dt_date, time_value: dt_time, weight: float) -> str:
    """Return a stable key uniquely identifying a measurement."""

    time_str = time_value.strftime("%H:%M:%S")
    return f"{date_value.isoformat()}T{time_str}|{float(weight):.3f}"


def prune_state_keys(keys: Iterable[str], limit: int = STATE_MAX_ENTRIES) -> list[str]:
    """Limit the amount of stored state while keeping order deterministic."""

    unique_keys = {str(key) for key in keys if key}

    def sort_key(item: str) -> datetime:
        dt_part, _, _ = item.partition("|")
        try:
            return datetime.fromisoformat(dt_part)
        except ValueError:
            return datetime.min

    ordered = sorted(unique_keys, key=sort_key)
    if len(ordered) > limit:
        ordered = ordered[-limit:]
    return ordered


# ---------------------------------------------------------------------------
# CSV handling
# ---------------------------------------------------------------------------

def load_weight_rows(csv_path: Path, encoding: str = CSV_ENCODING) -> pd.DataFrame:
    """Read Google Fit CSV and normalise the relevant columns.

    Returns a DataFrame containing the columns ``date``, ``time`` and
    ``weight_kg``.
    """

    print(f"[INFO] Selected CSV: {csv_path}")
    df_raw = pd.read_csv(csv_path, encoding=encoding)

    date_col = _find_column(df_raw, "date")
    datetime_col = _find_column(df_raw, "datetime")

    if not date_col and not datetime_col:
        raise ValueError("CSV에 '날짜' 또는 전체 일시(KST) 컬럼이 없습니다.")

    time_col = _find_column(df_raw, "time")
    weight_col = _find_column(df_raw, "weight")
    if not weight_col:
        raise ValueError("CSV에 '몸무게' 또는 'weight' 컬럼이 없습니다.")

    rows: list[dict[str, object]] = []
    for _, row in df_raw.iterrows():
        combined = _parse_datetime(row.get(datetime_col)) if datetime_col else None

        if combined is not None:
            d = combined.date()
            time_component = combined.timetz() if combined.tzinfo else combined.time()
            t = time_component.replace(tzinfo=None, microsecond=0)
        else:
            d = _parse_date(row.get(date_col)) if date_col else None
            t = _parse_time(row.get(time_col)) if time_col else dt_time(0, 0, 0)

        if d is None:
            continue

        weight = _parse_float(row.get(weight_col))
        if weight is None or weight <= 0:
            continue

        rows.append(
            {
                "date": d,
                "time": t,
                "weight_kg": round(float(weight), 3),
            }
        )

    if not rows:
        raise ValueError("유효한(>0) 몸무게 값이 없습니다.")

    df = pd.DataFrame(rows)
    df = df.sort_values(["date", "time"]).reset_index(drop=True)
    return df


def deduplicate_measurements(df: pd.DataFrame) -> pd.DataFrame:
    """Remove duplicate measurements based on date, time and weight."""

    if df.empty:
        return df.copy()

    seen: set[str] = set()
    unique_records: list[dict[str, object]] = []
    for record in df.to_dict(orient="records"):
        key = measurement_key(record["date"], record["time"], record["weight_kg"])
        if key in seen:
            continue
        seen.add(key)
        unique_records.append(record)

    return pd.DataFrame(unique_records, columns=df.columns).reset_index(drop=True)


def filter_new_entries(
    df: pd.DataFrame, uploaded_keys: set[str]
) -> tuple[pd.DataFrame, int]:
    """Drop rows that have already been uploaded according to ``uploaded_keys``."""

    if not uploaded_keys:
        return df.reset_index(drop=True), 0

    records = []
    skipped = 0
    for record in df.to_dict(orient="records"):
        key = measurement_key(record["date"], record["time"], record["weight_kg"])
        if key in uploaded_keys:
            skipped += 1
        else:
            records.append(record)

    filtered = pd.DataFrame(records, columns=df.columns).reset_index(drop=True)
    return filtered, skipped


# ---------------------------------------------------------------------------
# State persistence
# ---------------------------------------------------------------------------

def load_uploaded_keys(state_path: Path) -> set[str]:
    """Load previously uploaded measurement keys."""

    try:
        data = json.loads(state_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return set()
    except json.JSONDecodeError as exc:
        print(
            f"[WARN] State file {state_path} is corrupt ({exc}). Ignoring it.",
            file=sys.stderr,
        )
        return set()

    if isinstance(data, dict):
        keys = data.get("uploaded_keys") or data.get("keys") or []
    elif isinstance(data, list):
        keys = data
    else:
        keys = []

    return {str(key) for key in keys}


def save_uploaded_keys(state_path: Path, keys: Iterable[str]) -> None:
    """Persist uploaded measurement keys to disk."""

    state_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": STATE_VERSION,
        "saved_at": datetime.now(timezone.utc).isoformat(),
        "uploaded_keys": prune_state_keys(keys, STATE_MAX_ENTRIES),
    }
    state_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


# ---------------------------------------------------------------------------
# Garmin Connect interaction
# ---------------------------------------------------------------------------

def ensure_login(email: str, password: str, token_dir: Path) -> "Garmin":
    """Authenticate with Garmin Connect, reusing cached tokens when possible."""

    from garminconnect import Garmin

    token_dir.mkdir(parents=True, exist_ok=True)
    api = Garmin(email, password)
    api.login(tokenstore=str(token_dir))
    api.garth.save(str(token_dir))

    username = getattr(api.garth, "username", None) or email
    print(f"[INFO] Logged in as: {username}")
    return api


def resolve_timezone(name: Optional[str]) -> ZoneInfo | timezone:
    if name:
        try:
            tz = ZoneInfo(name)
        except Exception as exc:  # pragma: no cover - defensive: rare path
            raise SystemExit(f"[ERROR] Unknown timezone '{name}': {exc}")
        return tz

    tzinfo = datetime.now().astimezone().tzinfo
    return tzinfo or timezone.utc


def upload_measurements(
    api: "Garmin",
    df: pd.DataFrame,
    tzinfo: ZoneInfo | timezone,
    *,
    dry_run: bool = False,
) -> tuple[int, int, list[str]]:
    """Upload the provided measurements to Garmin Connect."""

    successes = 0
    failures = 0
    uploaded: list[str] = []

    for record in df.to_dict(orient="records"):
        dt_local = datetime.combine(record["date"], record["time"], tzinfo=tzinfo)
        timestamp_iso = dt_local.isoformat()
        weight = float(record["weight_kg"])
        key = measurement_key(record["date"], record["time"], weight)

        print(f"[INFO] Uploading {timestamp_iso} -> {weight:.3f} kg", flush=True)

        if dry_run:
            successes += 1
            uploaded.append(key)
            continue

        try:
            response = api.add_weigh_in(weight=weight, timestamp=timestamp_iso)

            uploaded.append(key)
            successes += 1

            if response:
                try:
                    snippet = json.dumps(response, ensure_ascii=False)
                except (TypeError, ValueError):
                    snippet = str(response)
                print(f"[OK] Response: {snippet[:160]}")
        except Exception as exc:  # pragma: no cover - network errors are external
            failures += 1
            print(
                f"[FAIL] {timestamp_iso} -> {weight:.3f} kg ({exc})",
                file=sys.stderr,
            )

    return successes, failures, uploaded


# ---------------------------------------------------------------------------
# Command-line interface
# ---------------------------------------------------------------------------

def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--csv",
        dest="csv_path",
        help="Path to a Google Fit CSV file. If omitted the newest matching file is used.",
    )
    parser.add_argument(
        "--pattern",
        dest="patterns",
        action="append",
        help="Glob pattern(s) for auto-detection (default: %(default)s)",
        default=list(DEFAULT_PATTERNS),
    )
    parser.add_argument(
        "--search-root",
        dest="search_root",
        default=".",
        help="Directory where CSV patterns are evaluated (default: current directory).",
    )
    parser.add_argument(
        "--state",
        dest="state_path",
        default=str(DEFAULT_STATE_PATH),
        help="Path to JSON file tracking uploaded measurements (default: %(default)s).",
    )
    parser.add_argument(
        "--no-state",
        dest="use_state",
        action="store_false",
        help="Disable state tracking and upload all measurements every run.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show the planned uploads without contacting Garmin Connect.",
    )
    parser.add_argument(
        "--max-uploads",
        type=int,
        dest="max_uploads",
        help="Limit the number of uploads in a single run (most recent entries only).",
    )
    parser.add_argument(
        "--timezone",
        dest="timezone_name",
        help="Timezone name (e.g. Asia/Seoul) used for timestamps. Defaults to local timezone.",
    )
    parser.add_argument(
        "--token-dir",
        dest="token_dir",
        help="Directory for Garmin authentication tokens (default: %(default)s)",
        default=str(TOKEN_DIR),
    )
    parser.add_argument(
        "--encoding",
        dest="encoding",
        default=CSV_ENCODING,
        help="CSV character encoding (default: %(default)s)",
    )
    parser.add_argument("--email", dest="email", help="Garmin Connect email (defaults to GARMIN_EMAIL).")
    parser.add_argument(
        "--password",
        dest="password",
        help="Garmin Connect password (defaults to GARMIN_PASSWORD).",
    )
    return parser


def find_latest_csv(patterns: Sequence[str], search_root: Path) -> Optional[Path]:
    candidates: list[tuple[float, Path]] = []
    for pattern in patterns:
        for path in search_root.glob(pattern):
            if path.is_file():
                try:
                    candidates.append((path.stat().st_mtime, path))
                except OSError:
                    continue
    if not candidates:
        return None
    candidates.sort(key=lambda item: item[0], reverse=True)
    return candidates[0][1]


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    email = args.email or os.getenv("GARMIN_EMAIL")
    password = args.password or os.getenv("GARMIN_PASSWORD")

    if not email or not password:
        print(
            "[ERROR] GARMIN_EMAIL/GARMIN_PASSWORD 환경변수 또는 --email/--password 옵션이 필요합니다.",
            file=sys.stderr,
        )
        return 2

    token_dir = Path(args.token_dir).expanduser()
    tzinfo = resolve_timezone(args.timezone_name)

    if args.csv_path:
        csv_path = Path(args.csv_path).expanduser()
        if not csv_path.is_file():
            print(f"[ERROR] CSV 파일을 찾을 수 없습니다: {csv_path}", file=sys.stderr)
            return 3
    else:
        search_root = Path(args.search_root).expanduser()
        csv_path = find_latest_csv(args.patterns, search_root)
        if not csv_path:
            print("[ERROR] 지정된 패턴과 일치하는 CSV를 찾지 못했습니다.", file=sys.stderr)
            return 3

    df = load_weight_rows(csv_path, encoding=args.encoding)
    print(f"[INFO] CSV에서 {len(df)}개 측정값을 읽었습니다.")

    selected = deduplicate_measurements(df)
    removed = len(df) - len(selected)
    if removed:
        print(f"[INFO] 중복된 측정 {removed}개를 제외했습니다.")

    uploaded_keys: set[str] = set()
    if args.use_state:
        state_path = Path(args.state_path).expanduser()
        uploaded_keys = load_uploaded_keys(state_path)
        selected, skipped = filter_new_entries(selected, uploaded_keys)
        if skipped:
            print(f"[INFO] {skipped}개 측정값은 이미 업로드되어 건너뜁니다.")
    else:
        state_path = None

    if args.max_uploads is not None and len(selected) > args.max_uploads:
        selected = selected.tail(args.max_uploads).reset_index(drop=True)
        print(f"[INFO] 최근 {args.max_uploads}개 항목만 업로드합니다.")

    if selected.empty:
        print("[INFO] 업로드할 새로운 측정값이 없습니다.")
        return 0

    print("[INFO] Upload candidates:")
    for record in selected.to_dict(orient="records"):
        print(
            f"  - {record['date'].isoformat()} {record['time'].strftime('%H:%M:%S')}"
            f" {record['weight_kg']:.3f} kg"
        )

    api = ensure_login(email, password, token_dir)

    successes, failures, keys = upload_measurements(api, selected, tzinfo, dry_run=args.dry_run)

    if not args.dry_run and state_path and keys:
        uploaded_keys.update(keys)
        save_uploaded_keys(state_path, uploaded_keys)
        print(f"[INFO] State updated: {state_path}")

    if failures:
        print(f"[WARN] {failures}개 항목 업로드 실패", file=sys.stderr)
        return 1

    print(f"[INFO] 업로드 완료: {successes}개 항목")
    return 0


if __name__ == "__main__":
    sys.exit(main())

