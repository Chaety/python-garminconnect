# -*- coding: utf-8 -*-
"""
Clean reset: Drive CSV -> Garmin weight
- CSV 최신 파일 1개 선택
- 컬럼: 날짜(필수), 몸무게(필수), 시간(선택)  [한국어 컬럼명 고정]
- 시간은 KST로 파싱 후 UTC로 변환 시도
- 라이브러리 버전에 따라:
    1) add_weigh_in_with_timestamps(weight, timestamp) 시도
    2) add_weigh_in(weight) 로 폴백
"""

import os
import sys
from pathlib import Path
from datetime import timezone, timedelta
import pandas as pd
from dateutil import parser as dateparser

DATE_COL = "날짜"
TIME_COL = "시간"
WEIGHT_COL = "몸무게"
KST = timezone(timedelta(hours=9))

def pick_latest_csv() -> Path:
    files = list(Path(".").glob("*.csv"))
    if not files:
        print("[ERROR] *.csv not found in workspace")
        sys.exit(1)
    latest = max(files, key=lambda p: p.stat().st_mtime)
    print(f"[INFO] Using CSV: {latest}")
    return latest

def read_csv(path: Path) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception:
            continue
    print("[ERROR] Failed to read CSV with encodings utf-8-sig/utf-8/cp949")
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

def main():
    # env
    email = os.getenv("GARMIN_EMAIL")
    password = os.getenv("GARMIN_PASSWORD")
    dry_run = os.getenv("DRY_RUN", "0") == "1"

    # csv
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

    ok, ng = 0, 0
    for i, row in df.iterrows():
        ts = parse_ts(row.get(DATE_COL), row.get(TIME_COL) if TIME_COL in df.columns else None)
        weight = to_float(row.get(WEIGHT_COL))
        if ts is None or weight is None:
            print(f"[SKIP] {i}: ts/weight missing -> {row.get(DATE_COL)} {row.get(TIME_COL)} {row.get(WEIGHT_COL)}")
            ng += 1
            continue

        if dry_run:
            print(f"[DRY] {i}: {ts.isoformat()} UTC, {weight}kg")
            ok += 1
            continue

        # 1) with timestamp
        try:
            if hasattr(client, "add_weigh_in_with_timestamps"):
                client.add_weigh_in_with_timestamps(weight=weight, timestamp=ts.isoformat(timespec="seconds"))
                print(f"[OK] {i}: with timestamp -> {ts.isoformat()} UTC, {weight}")
                ok += 1
                continue
        except TypeError:
            pass
        except Exception as e:
            print(f"[WARN] with_timestamps failed: {e}")

        # 2) fallback: weight only
        try:
            client.add_weigh_in(weight=weight)
            print(f"[OK] {i}: weight only -> {weight}")
            ok += 1
        except Exception as e:
            print(f"[FAIL] {i}: {e}")
            ng += 1

    print(f"Done. success={ok}, failed={ng}")

if __name__ == "__main__":
    main()
