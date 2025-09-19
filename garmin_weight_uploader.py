#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
garmin_weight_uploader.py

- Google Drive / Google Fit에서 내려받은 한국어 CSV(예: "무게 2025.09.18 Google Fit.csv")를 읽어
  가민 커넥트에 과거 시각 포함해 체중을 업로드합니다.
- 수정 사항:
  * garth.Client에는 .session 속성이 없음 → 사용 제거
  * 인증 후 client.connectapi()로 가민 엔드포인트에 직접 POST

환경변수:
- GARMIN_EMAIL
- GARMIN_PASSWORD

필수 패키지:
- garth>=0.5
- pandas, python-dateutil
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Tuple

import pandas as pd
from dateutil import tz
import garth

# -------------------------
# 설정
# -------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# 가민 커넥트 Weight 업로드 엔드포인트(modern/proxy 이하 path만 사용)
WEIGHT_POST_PATH = "userprofile-service/userprofile/userprofile/weight"

# CSV에서 사용하는 한국어 컬럼명
COL_DATE = "날짜"
COL_TIME = "시간"
COL_WEIGHT = "몸무게"

# 기본 단위(kg)
UNIT_KEY = "kg"

# 타임존 후보
LOCAL_TZ_CANDIDATES = [
    os.getenv("LOCAL_TZ", "").strip(),
    tz.tzlocal(),
    tz.gettz("Asia/Seoul"),
]

# -------------------------
# 로깅
# -------------------------
logging.basicConfig(level=LOG_LEVEL, format="[%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# -------------------------
# 유틸
# -------------------------
def _first_valid_tz():
    for cand in LOCAL_TZ_CANDIDATES:
        if cand:
            return cand
    return tz.gettz("UTC")

LOCAL_TZ = _first_valid_tz()

def _read_latest_csv_in_cwd() -> str:
    csvs = sorted(
        (p for p in os.listdir(".") if p.lower().endswith(".csv")),
        key=lambda x: os.path.getmtime(x),
        reverse=True,
    )
    if not csvs:
        raise FileNotFoundError("작업 디렉터리에 CSV가 없습니다.")
    latest = csvs[0]
    logger.info("Selected CSV: %s", latest)
    return latest

def _to_float_safe(val) -> Optional[float]:
    if pd.isna(val):
        return None
    s = str(val).strip().replace(",", "")
    if s == "" or s.lower() in {"nan", "none"}:
        return None
    try:
        return float(s)
    except Exception:
        return None

def _parse_datetime_kr(date_cell: str, time_cell: Optional[str]) -> Tuple[datetime, str]:
    date_cell = (str(date_cell) if date_cell is not None else "").strip()
    time_cell = (str(time_cell) if time_cell is not None else "").strip()

    dt_str_candidates = []
    if ":" in date_cell:
        dt_str_candidates.append(date_cell)
    if date_cell and time_cell and ":" in time_cell and ":" not in date_cell:
        dt_str_candidates.append(f"{date_cell} {time_cell}")

    fmts = [
        "%Y.%m.%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%Y.%m.%d %H:%M",
        "%Y-%m-%d %H:%M",
        "%Y/%m/%d %H:%M",
    ]

    dt_local = None
    for s in (c for c in dt_str_candidates if c):
        for fmt in fmts:
            try:
                dt_naive = datetime.strptime(s, fmt)
                dt_local = dt_naive.replace(tzinfo=LOCAL_TZ)
                break
            except ValueError:
                continue
        if dt_local:
            break

    if dt_local is None:
        date_fmts = ["%Y.%m.%d", "%Y-%m-%d", "%Y/%m/%d"]
        d_only = None
        for fmt in date_fmts:
            try:
                d_only = datetime.strptime(date_cell, fmt)
                break
            except ValueError:
                continue

        t_only = None
        if time_cell:
            for fmt in ["%H:%M:%S", "%H:%M"]:
                try:
                    t_only = datetime.strptime(time_cell, fmt)
                    break
                except ValueError:
                    continue

        if d_only is not None:
            if t_only is not None:
                dt_naive = datetime(
                    d_only.year, d_only.month, d_only.day,
                    t_only.hour, t_only.minute,
                    getattr(t_only, "second", 0),
                )
            else:
                dt_naive = datetime(d_only.year, d_only.month, d_only.day, 0, 0, 0)
            dt_local = dt_naive.replace(tzinfo=LOCAL_TZ)

    if dt_local is None:
        raise ValueError(f"날짜/시간 파싱 실패: date='{date_cell}', time='{time_cell}'")

    date_str = dt_local.strftime("%Y-%m-%d")
    return dt_local, date_str

def _epoch_millis_utc(dt_local_aware: datetime) -> int:
    dt_utc = dt_local_aware.astimezone(tz.UTC)
    return int(dt_utc.timestamp() * 1000)

def _post_weight_with_retries(client: garth.Client, payload: dict, retries: int = 3, backoff: float = 1.5):
    last_exc = None
    for i in range(1, retries + 1):
        try:
            # garth.Client.connectapi: modern/proxy/ 아래 path로 호출
            r = client.connectapi(WEIGHT_POST_PATH, method="POST", json=payload)
            status = getattr(r, "status_code", None)
            if status is None or status // 100 == 2:
                return r
            logger.warning("POST 실패(status=%s) - %s", status, getattr(r, "text", "")[:200])
        except Exception as e:
            last_exc = e
            logger.warning("POST 예외(%d/%d): %s", i, retries, e)
        time.sleep(backoff * i)
    if last_exc:
        raise last_exc
    raise RuntimeError("POST 재시도 초과")

@dataclass
class WeightRow:
    dt_local: datetime
    date_str: str
    weight_kg: float

def _row_from_series(row: pd.Series) -> Optional[WeightRow]:
    try:
        s = {k.strip(): row[k] for k in row.index}
        dt_local, date_str = _parse_datetime_kr(s.get(COL_DATE), s.get(COL_TIME))
        weight = _to_float_safe(s.get(COL_WEIGHT))
        if weight is None or weight <= 0:
            return None
        return WeightRow(dt_local=dt_local, date_str=date_str, weight_kg=weight)
    except Exception as e:
        logger.info("[SKIP] row %s: 파싱 실패 - %s", getattr(row, "name", "?"), e)
        return None

def _load_rows(csv_path: str) -> list[WeightRow]:
    df = pd.read_csv(csv_path)
    logger.info("Columns: %s", list(df.columns))
    rows: list[WeightRow] = []
    for _, r in df.iterrows():
        wr = _row_from_series(r)
        if wr:
            rows.append(wr)
    return rows

def _build_payload(w: WeightRow) -> dict:
    return {
        "timestampGMT": _epoch_millis_utc(w.dt_local),  # UTC epoch millis
        "date": w.date_str,                              # YYYY-MM-DD
        "unitKey": UNIT_KEY,
        "weight": round(w.weight_kg, 3),
    }

def login_client() -> garth.Client:
    email = os.environ.get("GARMIN_EMAIL", "").strip()
    password = os.environ.get("GARMIN_PASSWORD", "").strip()
    if not email or not password:
        raise RuntimeError("GARMIN_EMAIL / GARMIN_PASSWORD 환경변수 필요")

    client = garth.Client()
    client.login(email, password)
    logger.info("Garmin login OK")
    return client

def main():
    csv_path = _read_latest_csv_in_cwd()
    rows = _load_rows(csv_path)
    if not rows:
        logger.info("업로드할 유효한 행이 없습니다. 종료합니다.")
        return

    client = login_client()

    success = 0
    failed = 0
    for w in rows:
        payload = _build_payload(w)
        try:
            _post_weight_with_retries(client, payload)
            success += 1
        except Exception as e:
            failed += 1
            logger.info(
                "[FAIL] %s %.3fkg @ %s (%s) - %s",
                w.date_str,
                w.weight_kg,
                w.dt_local.isoformat(),
                payload.get("timestampGMT"),
                e,
            )

    logger.info("Done. success=%d, failed=%d", success, failed)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.exception(e)
        raise
