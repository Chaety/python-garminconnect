# -*- coding: utf-8 -*-
"""
Garmin Weight Uploader (diagnostic-friendly)
- 작업 디렉토리의 *.csv 중 최신 파일 선택
- '날짜' + '시간'(KST) → UTC 변환
- 라이브러리 버전에 따라 업로드 메서드 조합 자동 시도
- DRY_RUN=1 이면 실제 업로드 대신 파싱 결과를 로그로만 출력
"""

import os
import sys
import inspect
from pathlib import Path
from datetime import timezone, timedelta, datetime

import pandas as pd
from dateutil import parser as dateparser

DATE_COL   = "날짜"
TIME_COL   = "시간"
WEIGHT_COL = "몸무게"

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
    ts_text = str(date_val) if not time_val or str(time_val).strip() == "" else f"{date_val} {time_val}"
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

def try_upload(g, weight: float, ts_utc: datetime) -> str:
    iso_ms  = ts_utc.isoformat(timespec="milliseconds")
    date_s  = ts_utc.astimezone(LOCAL_TZ).strftime("%Y-%m-%d")
    time_s  = ts_utc.astimezone(LOCAL_TZ).strftime("%H:%M:%S")

    patterns = []
    if hasattr(g, "add_weigh_in_with_timestamps"):
        try: print("[DEBUG] sig add_weigh_in_with_timestamps:", inspect.signature(g.add_weigh_in_with_timestamps))
        except Exception: pass
        patterns += [
            ("add_weigh_in_with_timestamps", dict(weight=weight, timestamp=iso_ms)),
            ("add_weigh_in_with_timestamps", dict(weight=weight)),
        ]
    if hasattr(g, "add_weigh_in"):
        try: print("[DEBUG] sig add_weigh_in:", inspect.signature(g.add_weigh_in))
        except Exception: pass
        patterns += [
            ("add_weigh_in", dict(weight=weight, timestamp=iso_ms)),  # 일부 포크
            ("add_weigh_in", dict(weight=weight, date=date_s, time=time_s)),  # 가정
            ("add_weigh_in", dict(weight=weight, date=date_s)),            # 가정
            ("add_weigh_in", dict(weight=weight)),
        ]

    last_err = None
    for meth, kwargs in patterns:
        try:
            print(f"[DEBUG] try {meth} {kwargs}")
            getattr(g, meth)(**kwargs)
            return f"{meth} {kwargs}"
        except TypeError as te:
            print(f"[DEBUG] TypeError {meth}: {te}")
            last_err = te
        except Exception as e:
            print(f"[DEBUG] Exception {meth}: {e}")
            last_err = e

    raise RuntimeError(f"all patterns failed; last_err={last_err}")

def main():
    ensure_deps()
    from garminconnect import Garmin
    print("[INFO] garminconnect version:", __import__("garminconnect").__version__)

    email = os.getenv("GARMIN_EMAIL")
    password = os.getenv("GARMIN_PASSWORD")
    dry_run = os.getenv("DRY_RUN", "0") == "1"

    csv_path = pick_latest_csv()
    df = read_csv_safely(csv_path)
    print("[INFO] CSV columns:", list(df.columns))
    print("[INFO] CSV rows:", len(df))

    for col in (DATE_COL, WEIGHT_COL):
        if col not in df.columns:
            print(f"[ERROR] 필수 컬럼 누락: {col}")
            sys.exit(1)

    g = Garmin(email, password)
    g.login()
    print("[INFO] Garmin login OK")

    success = 0
    failed  = 0

    for idx, row in df.iterrows():
        ts_utc = parse_ts(row.get(DATE_COL), row.get(TIME_COL) if TIME_COL in df.columns else None)
        weight = to_float(row.get(WEIGHT_COL))
        print(f"[ROW] {idx} date={row.get(DATE_COL)} time={row.get(TIME_COL)} -> ts_utc={None if ts_utc is None else ts_utc.isoformat()} weight={weight}")

        if ts_utc is None or weight is None:
            print(f"[SKIP] {idx}: ts/weight 누락")
            failed += 1
            continue

        if dry_run:
            success += 1
            continue

        try:
            used = try_upload(g, weight, ts_utc)
            print(f"[OK] {idx}: used={used}")
            success +
