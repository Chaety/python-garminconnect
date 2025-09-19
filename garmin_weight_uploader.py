# -*- coding: utf-8 -*-
"""
Drive에서 받은 최신 CSV 1개를 읽어 Garmin Connect에 체중 업로드.
- 컬럼명(한글, 고정): 날짜(필수), 몸무게(필수), 시간(선택)
- 시간은 KST로 해석 후 UTC로 변환해 업로드 시도
- 순서:
    1) add_weigh_in_with_timestamps(weight, timestamp) 시도
    2) add_weigh_in(weight) 시도
- 일부 환경에서 서버가 204(No Content)를 돌려주면 라이브러리가 response.json()에서
  'Expecting value: line 1 column 1 (char 0)' 로 예외를 던짐 → 이 경우를 **성공으로 간주**.
- DRY_RUN=1 이면 업로드 대신 파싱 결과만 출력
"""

import os
import sys
from pathlib import Path
from datetime import timezone, timedelta
import pandas as pd
from dateutil import parser as dateparser

DATE_COL   = "날짜"
TIME_COL   = "시간"
WEIGHT_COL = "몸무게"
KST = timezone(timedelta(hours=9))

def pick_latest_csv() -> Path:
    files = sorted(Path(".").glob("*.csv"))
    if not files:
        print("[ERROR] *.csv not found in workspace")
        sys.exit(1)
    latest = max(files, key=lambda p: p.stat().st_mtime)
    print(f"[INFO] Selected CSV: {latest}")
    return latest

def read_csv(path: Path) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception:
            continue
    print("[ERROR] Failed to read CSV (utf-8-sig/utf-8/cp949 tried)")
    sys.exit(1)

def to_float(x):
    try:
        s = str(x).strip().replace(",", "").replace("%", "")
        return float(s) if s else None
    except Exception:
        return None

def parse_ts(date_val, time_val):
    if pd.isna(date_val) or str(date_val).strip() == "":
        return None
    text = str(date_val) if (time_val is None or str(time_val).strip() == "") else f"{date_val} {time_val}"
    dt = dateparser.parse(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=KST)
    return dt.astimezone(timezone.utc)

def is_json_empty_error(e: Exception) -> bool:
    """빈 응답(JSON 없음)일 때 흔히 보이는 에러 문자열 판별."""
    msg = str(e)
    return "Expecting value" in msg and "line 1 column 1" in msg

def main():
    email = os.getenv("GARMIN_EMAIL")
    password = os.getenv("GARMIN_PASSWORD")
    dry_run = os.getenv("DRY_RUN", "0") == "1"

    csv_path = pick_latest_csv()
    df = read_csv(csv_path)
    print("[INFO] Columns:", list(df.columns))

    for col in (DATE_COL, WEIGHT_COL):
        if col not in df.columns:
            print(f"[ERROR] Missing column: {col}")
            sys.exit(1)

    from garminconnect import Garmin
    client = Garmin(email, password)
    client.login()
    print("[INFO] Garmin login OK")

    success, failed = 0, 0

    for i, row in df.iterrows():
        ts_utc = parse_ts(row.get(DATE_COL), row.get(TIME_COL) if TIME_COL in df.columns else None)
        weight = to_float(row.get(WEIGHT_COL))
        if ts_utc is None or weight is None:
            print(f"[SKIP] {i}: ts/weight missing -> {row.get(DATE_COL)} {row.get(TIME_COL)} {row.get(WEIGHT_COL)}")
            failed += 1
            continue

        if dry_run:
            print(f"[DRY] {i}: {ts_utc.isoformat()} UTC, {weight}kg")
            success += 1
            continue

        # 1) timestamp 지원 시도
        try:
            if hasattr(client, "add_weigh_in_with_timestamps"):
                client.add_weigh_in_with_timestamps(
                    weight=weight,
                    timestamp=ts_utc.isoformat(timespec="milliseconds"),
                )
                print(f"[OK] {i}: with timestamp -> {ts_utc.isoformat()} UTC, {weight}kg")
                success += 1
                continue
        except TypeError:
            pass
        except Exception as e:
            if is_json_empty_error(e):
                # 서버가 204 반환 → 성공으로 간주
                print(f"[OK*] {i}: with timestamp (204 No Content assumed) -> {ts_utc.isoformat()} UTC, {weight}kg")
                success += 1
                continue
            print(f"[WARN] with_timestamps failed: {e}")

        # 2) weight only
        try:
            client.add_weigh_in(weight=weight)
            print(f"[OK] {i}: weight only -> {weight}kg")
            success += 1
            continue
        except Exception as e:
            if is_json_empty_error(e):
                print(f"[OK*] {i}: weight only (204 No Content assumed) -> {weight}kg")
                success += 1
                continue
            print(f"[FAIL] {i}: {e}")
            failed += 1

    print(f"Done. success={success}, failed={failed}")

if __name__ == "__main__":
    main()
