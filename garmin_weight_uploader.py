# garmin_weight_uploader.py (전체 교체)
from __future__ import annotations
import os, time, json
from dataclasses import dataclass
from typing import Tuple
from datetime import datetime
import pandas as pd
from dateutil import tz
import requests
import garth

# ---- 설정 ----
COL_DATE = "날짜"
COL_TIME = "시간"
COL_WEIGHT = "몸무게"
CSV_GLOB = "*.csv"
LOCAL_TZ = os.environ.get("LOCAL_TZ", "Asia/Seoul")
WEIGHT_POST_URL = "https://connect.garmin.com/modern/proxy/weight-service/user-weight"
MAX_RETRIES = 4
BASE_SLEEP = 1.5
# --------------

@dataclass
class UploadResult:
    ok: bool
    status: int
    text: str
    attempt: int

def _read_latest_csv() -> Tuple[str, pd.DataFrame]:
    import glob, os
    files = sorted(glob.glob(CSV_GLOB), key=os.path.getmtime, reverse=True)
    if not files:
        raise FileNotFoundError("CSV (*.csv) 파일이 없습니다.")
    csv = files[0]
    df = pd.read_csv(csv)
    return csv, df

def _try_parse_dt(s: str, patterns: list[str]) -> datetime | None:
    for p in patterns:
        try:
            return datetime.strptime(s, p)
        except ValueError:
            continue
    return None

def _parse_row_to_dt_kg(row) -> Tuple[datetime, float]:
    """행에서 (현지 시각 datetime(타임존 포함), kg) 반환."""
    date_s = str(row.get(COL_DATE, "")).strip()
    time_s = str(row.get(COL_TIME, "")).strip()
    w_s = str(row.get(COL_WEIGHT, "")).strip().replace('"', '').replace(',', '')

    if not date_s:
        raise ValueError("날짜 열이 비어있습니다.")
    if not w_s:
        raise ValueError("몸무게 열이 비어있습니다.")

    # 1) 날짜열이 'YYYY.MM.DD HH:MM:SS' 또는 'YYYY-MM-DD HH:MM:SS' 인 경우
    dt_local = _try_parse_dt(date_s,
        ["%Y.%m.%d %H:%M:%S", "%Y-%m-%d %H:%M:%S"])
    # 2) 날짜만 있는 경우, 시간열을 붙여서 파싱
    if dt_local is None:
        # 시간열이 비었으면 00:00:00
        if not time_s:
            time_s = "00:00:00"
        joined = f"{date_s} {time_s}".strip()
        dt_local = _try_parse_dt(joined, [
            "%Y.%m.%d %H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
        ])
    # 3) 마지막으로 날짜만이라도 파싱해서 00:00:00 부여
    if dt_local is None:
        only_date = _try_parse_dt(date_s, ["%Y.%m.%d", "%Y-%m-%d"])
        if only_date is not None:
            dt_local = datetime(
                only_date.year, only_date.month, only_date.day, 0, 0, 0
            )
    if dt_local is None:
        raise ValueError(f"날짜/시간 파싱 실패: date='{date_s}', time='{time_s}'")

    # 타임존 부여
    dt_local = dt_local.replace(tzinfo=tz.gettz(LOCAL_TZ))

    # 몸무게 kg 파싱
    if w_s.lower().endswith("kg"):
        w_s = w_s[:-2]
    kg = float(w_s.strip())

    return dt_local, kg

def _payloads_for(dt_local: datetime, kg: float) -> Tuple[dict, dict]:
    dt_utc = dt_local.astimezone(tz.UTC)
    payload1 = {
        "timestampGMT": dt_utc.strftime("%Y-%m-%dT%H:%M:%S.000"),
        "weight": kg,
        "unitKey": "kg",
        "sourceType": "MANUAL",
    }
    payload2 = {
        "date": dt_local.strftime("%Y-%m-%d"),
        "time": dt_local.strftime("%H:%M:%S"),
        "weight": kg,
        "unitKey": "kg",
        "sourceType": "MANUAL",
    }
    return payload1, payload2

def _post_with_retries(sess: requests.Session, url: str, body: dict) -> UploadResult:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = sess.post(url, json=body, timeout=30)
            code = r.status_code
            if code in (200, 201, 204):
                return UploadResult(True, code, r.text, attempt)
            if code in (409, 429) or 500 <= code < 600:
                time.sleep(BASE_SLEEP * (2 ** (attempt - 1)))
                continue
            return UploadResult(False, code, r.text, attempt)
        except requests.RequestException as e:
            if attempt == MAX_RETRIES:
                return UploadResult(False, -1, repr(e), attempt)
            time.sleep(BASE_SLEEP * (2 ** (attempt - 1)))
    return UploadResult(False, -1, "unknown", MAX_RETRIES)

def main():
    email = os.environ.get("GARMIN_EMAIL")
    password = os.environ.get("GARMIN_PASSWORD")
    if not email or not password:
        raise SystemExit("환경변수 GARMIN_EMAIL / GARMIN_PASSWORD 필요")

    csv_file, df = _read_latest_csv()
    print(f"[INFO] Selected CSV: {csv_file}")
    print(f"[INFO] Columns: {list(df.columns)}")

    client = garth.Client()
    client.login(email, password)
    sess = client.connectapi
    print("[INFO] Garmin login OK")

    success = 0
    failed = 0
    for idx, row in df.iterrows():
        try:
            dt_local, kg = _parse_row_to_dt_kg(row)
        except Exception as e:
            print(f"[SKIP] row {idx}: 파싱 실패 - {e}")
            failed += 1
            continue

        p1, p2 = _payloads_for(dt_local, kg)
        res = _post_with_retries(sess, WEIGHT_POST_URL, p1)
        if not res.ok:
            res2 = _post_with_retries(sess, WEIGHT_POST_URL, p2)
            if res2.ok:
                success += 1
                print(f"[OK]  row {idx}: {kg}kg @ {dt_local.isoformat()}  ({res2.status}, attempt={res2.attempt})")
            else:
                failed += 1
                print(f"[FAIL] row {idx}: {kg}kg @ {dt_local.isoformat()}  "
                      f"p1={res.status}/{res.text[:120]!r}, p2={res2.status}/{res2.text[:120]!r}")
        else:
            success += 1
            print(f"[OK]  row {idx}: {kg}kg @ {dt_local.isoformat()}  ({res.status}, attempt={res.attempt})")

    print(f"Done. success={success}, failed={failed}")

if __name__ == "__main__":
    main()
