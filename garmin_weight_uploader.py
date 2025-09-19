# -*- coding: utf-8 -*-
"""
Garmin Weight Uploader
- 작업 디렉토리의 *.csv 중 가장 최신 파일 자동 선택
- '날짜' + '시간'을 KST로 파싱 → UTC ISO8601로 변환
- 가민 커넥트에 무게만 업로드 (percent_fat, bmi 미사용)
- 필수 컬럼: 날짜, 몸무게
- 선택 컬럼: 시간
"""

import os
import sys
from pathlib import Path
from datetime import timezone, timedelta

import pandas as pd
from dateutil import parser as dateparser

# ===== CSV 컬럼명 =====
DATE_COL   = "날짜"
TIME_COL   = "시간"
WEIGHT_COL = "몸무게"

# ===== 타임존(KST) =====
LOCAL_TZ = timezone(timedelta(hours=9))  # Asia/Seoul

def to_float(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        s = str(val).strip().replace(",", "").replace("%", "")
        return float(s) if s else None
    except Exception:
        return None

def parse_ts(date_val, time_val):
    if pd.isna(date_val) or str(date_val).strip() == "":
        return None
    if time_val is None or (isinstance(time_val, float) and pd.isna(time_val)) or str(time_val).strip() == "":
        ts_text = str(date_val)
    else:
        ts_text = f"{date_val} {time_val}"
    dt = dateparser.parse(ts_text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=LOCAL_TZ)
    return dt.astimezone(timezone.utc)

def read_csv_safely(path: Path) -> pd.DataFrame:
    last = None
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception as e:
            last = e
    print("[ERROR] CSV 읽기 실패:", path, "; last err:", last)
    sys.exit(1)

def pick_latest_csv() -> Path:
    candidates = sorted(Path(".").glob("*.csv"))
    if not candidates:
        print("[ERROR] 작업 디렉토리에 *.csv 없음")
        sys.exit(1)
    latest = max(candidates, key=lambda p: p.stat().st_mtime)
    print(f"[INFO] 선택된 CSV: {latest}")
    return latest

def ensure_deps():
    for dep in ("garminconnect", "pytz", "dateutil", "pandas"):
        try:
            __import__(dep)
        except ImportError:
            print(f"[ERROR] {dep} 미설치")
            sys.exit(1)

def main():
    ensure_deps()
    from garminconnect import Garmin

    email = os.getenv("GARMIN_EMAIL")
    password = os.getenv("GARMIN_PASSWORD")
    if not email or not password:
        print("[WARN] GARMIN_EMAIL / GARMIN_PASSWORD 미설정")

    dry_run = os.getenv("DRY_RUN", "0") == "1"

    csv_path = pick_latest_csv()
    df = read_csv_safely(csv_path)

    # 필수 컬럼 확인
    for col in (DATE_COL, WEIGHT_COL):
        if col not in df.columns:
            print(f"[ERROR] 필수 컬럼 누락: {col}, CSV 보유 컬럼: {list(df.columns)}")
            sys.exit(1)

    g = Garmin(email, password)
    g.login()

    success = 0
    failed = 0

    for idx, row in df.iterrows():
        ts_utc = parse_ts(
            row.get(DATE_COL),
            row.get(TIME_COL) if TIME_COL in df.columns else None
        )
        weight = to_float(row.get(WEIGHT_COL))

        if ts_utc is None or weight is None:
            print(f"[SKIP] {idx}: ts/weight 누락")
            failed += 1
            continue

        if dry_run:
            print(f"[DRY] {idx}: {ts_utc.isoformat()}, weight={weight}")
            success += 1
            continue

        try:
            # 가능한 경우 timestamp 포함, 안 되면 weight만 업로드
            if hasattr(g, "add_weigh_in_with_timestamps"):
                try:
                    g.add_weigh_in_with_timestamps(
                        weight=weight,
                        timestamp=ts_utc.isoformat(timespec="milliseconds")
                    )
                except TypeError:
                    g.add_weigh_in(weight=weight)
            else:
                g.add_weigh_in(weight=weight)

            print(f"[OK] {idx}: {ts_utc.isoformat()} UTC, {weight}kg")
            success += 1

        except Exception as e:
            print(f"[FAIL] {idx}: {e}")
            failed += 1

    print(f"Done. success={success}, failed={failed}")

if __name__ == "__main__":
    main()
