#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
garmin_weight_uploader.py

- Google Drive / Google Fit에서 내려받은 한국어 CSV(예: "무게 2025.09.18 Google Fit.csv")를 읽어
  가민 커넥트에 과거 시각 포함해 체중을 업로드합니다.
- **수정 사항**: garth.Client.connectapi()는 세션이 아니라 엔드포인트 호출 메서드였습니다.
  인증된 requests.Session 은 client.session 으로 가져와 사용합니다.
- CSV 파싱은 다음을 모두 허용합니다.
  1) '날짜' 컬럼에 "YYYY.MM.DD HH:MM:SS"가 들어있는 형태
  2) '날짜'는 날짜만, '시간'에 HH:MM:SS가 들어있는 형태
  3) 따옴표가 있는 숫자 문자열("70.2") 등

환경변수:
- GARMIN_EMAIL
- GARMIN_PASSWORD

필수 패키지:
- garth>=0.5
- pandas, python-dateutil
"""

from __future__ import annotations

import json
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

# 가민 커넥트 Weight 업로드 엔드포인트
WEIGHT_POST_URL = (
    "https://connect.garmin.com/modern/proxy/userprofile-service/userprofile/userprofile/weight"
)

# CSV에서 사용하는 한국어 컬럼명(일부 앱/내보내기 변형 대응)
COL_DATE = "날짜"
COL_TIME = "시간"
COL_WEIGHT = "몸무게"

# 기본 단위(kg)
UNIT_KEY = "kg"

# 과거 시각 업로드 시 타임존: CSV가 현지(예: Asia/Seoul) 기준이라면 해당 타임존을 지정
LOCAL_TZ_CANDIDATES = [
    os.getenv("LOCAL_TZ", "").strip(),
    tz.tzlocal(),
    tz.gettz("Asia/Seoul"),
]


# -------------------------
# 로깅
# -------------------------
logging.basicConfig(
    level=LOG_LEVEL,
    format="[%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


# -------------------------
# 유틸
# -------------------------
def _first_valid_tz() -> tz.tzfile | tz.tzlocal:
    for cand in LOCAL_TZ_CANDIDATES:
        if cand:
            return cand
    return tz.gettz("UTC")


LOCAL_TZ = _first_valid_tz()


def _read_latest_csv_in_cwd() -> str:
    """현재 작업 폴더에서 가장 최근 CSV 하나 선택."""
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
    """따옴표 포함 문자열 등 다양한 입력을 float로 안전 변환."""
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
    """
    한국어 CSV의 날짜/시간을 파싱하여 '현지 타임존 aware' datetime 생성 후,
    UTC epoch millis(timestampGMT) 계산에 쓰기 위해 반환.

    허용 예:
    - date_cell: "2025.09.18 12:57:00", time_cell: "12:57:00"  (time_cell은 무시)
    - date_cell: "2025.09.18",         time_cell: "12:57:00"
    - date_cell: "2025-09-18",         time_cell: "12:57:00"
    - date_cell: "2025/09/18 03:19:22", time_cell: None

    반환: (dt_local_aware, date_str_yyyy_mm_dd)
    """
    date_cell = (str(date_cell) if date_cell is not None else "").strip()
    time_cell = (str(time_cell) if time_cell is not None else "").strip()

    dt_str_candidates = []
    if any(sep in date_cell for sep in [":"]):
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
            time_fmts = ["%H:%M:%S", "%H:%M"]
            for fmt in time_fmts:
                try:
                    t_only = datetime.strptime(time_cell, fmt)
                    break
                except ValueError:
                    continue

        if d_only is not None:
            if t_only is not None:
                dt_naive = datetime(
                    d_only.year,
                    d_only.month,
                    d_only.day,
                    t_only.hour,
                    t_only.minute,
                    t_only.second if hasattr(t_only, "second") else 0,
                )
            else:
                dt_naive = datetime(d_only.year, d_only.month, d_only.day, 0, 0, 0)

            dt_local = dt_naive.replace(tzinfo=LOCAL_TZ)

    if dt_local is None:
        raise ValueError(f"날짜/시간 파싱 실패: date='{date_cell}', time='{time_cell}'")

    date_str = dt_local.strftime("%Y-%m-%d")
    return dt_local, date_str


def _epoch_millis_utc(dt_local_aware: datetime) -> int:
    """현지 aware datetime → UTC epoch millis."""
    dt_utc = dt_local_aware.astimezone(tz.UTC)
    return int(dt_utc.timestamp() * 1000)


def _post_with_retries(sess, url: str, body: dict, retries: int = 3, backoff: float = 1.5):
    """간단한 재시도 POST 래퍼."""
    last_exc = None
    for i in range(1, retries + 1):
        try:
            r = sess.post(url, json=body, timeout=30)
            if r.status_code // 100 == 2:
                return r
            logger.warning("POST 실패(status=%s) - %s", r.status_code, r.text[:200])
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
    """CSV 한 행 → WeightRow. 파싱 실패/무게 없음이면 None."""
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
    """
    가민 커넥트 Weight 업로드 페이로드.
    - timestampGMT: UTC epoch millis
    - date: YYYY-MM-DD (현지 기준 날짜 필드, 가민이 표시용으로 사용)
    - unitKey: "kg"
    - weight: kg 값
    """
    return {
        "timestampGMT": _epoch_millis_utc(w.dt_local),
        "date": w.date_str,
        "unitKey": UNIT_KEY,
        "weight": round(w.weight_kg, 3),
    }


def login_and_session() -> Tuple[garth.Client, object]:
    """
    garth로 로그인 후 **인증된 requests.Session** 반환.
    중요: 세션은 client.session 으로 가져옵니다.
    """
    email = os.environ.get("GARMIN_EMAIL", "").strip()
    password = os.environ.get("GARMIN_PASSWORD", "").strip()
    if not email or not password:
        raise RuntimeError("GARMIN_EMAIL / GARMIN_PASSWORD 환경변수 필요")

    client = garth.Client()
    client.login(email, password)

    # 인증된 세션
    sess = client.session  # ← 핵심 수정

    # 가민이 기대하는 헤더 일부가 비어있을 경우 보강(무해)
    sess.headers.setdefault("Origin", "https://connect.garmin.com")
    sess.headers.setdefault("Referer", "https://connect.garmin.com/")
    sess.headers.setdefault("Accept", "application/json, text/plain, */*")

    logger.info("Garmin login OK")
    return client, sess


def main():
    csv_path = _read_latest_csv_in_cwd()
    rows = _load_rows(csv_path)

    if not rows:
        logger.info("업로드할 유효한 행이 없습니다. 종료합니다.")
        return

    client, sess = login_and_session()

    success = 0
    failed = 0
    for w in rows:
        payload = _build_payload(w)
        try:
            res = _post_with_retries(sess, WEIGHT_POST_URL, payload)
            logger.debug("POST OK %s", getattr(res, "status_code", "?"))
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
