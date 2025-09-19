# -*- coding: utf-8 -*-
"""
Garmin Weight Uploader (diagnostic)
- 작업 폴더의 *.csv 중 최신 파일 선택
- '날짜' + '시간'(KST) -> UTC 변환
- 가민 라이브러리 버전에 따라 가능한 업로드 시그니처를 자동 시도
- DRY_RUN=1 이면 실제 업로드 대신 파싱/시그니처 로그만 출력
"""

import os
import sys
import inspect
from pathlib import Path
from datetime import timezone, timedelta, datetime

import pandas as pd
from dateutil import parser as dateparser

DATE_COL = "날짜"
TIME_COL = "시간"
WEIGHT_COL = "몸무게"

LOCAL_TZ = timezone(timedelta(hours=9))  # Asia/Seoul

def to_float(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    try:
        s = str(v).strip().replace(",", "").replace("%", "")
        return float(s) if s else None
    except Exception:
        return None

def parse_ts(date_val, time_val):
    if pd.isna(date_val) or str(date_val).strip() == "":
        return None
    text = str(date_val) if (time_val is None or str(time_val).strip() == "") else f"{date_val} {time_val}"
    dt = dateparser.parse(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=LOCAL_TZ)
    return dt.astimezone(timezone.utc)

def read_csv(path: Path) -> pd.DataFrame:
    last = None
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception as e:
            last = e
    print("[ERROR] CSV read failed:", path, "; last:", last)
    sys.exit(1)

def pick_latest_csv() -> Path:
    cands = sorted(Path(".").glob("*.csv"))
    if not cands:
        print("[ERROR] No *.csv in workspace")
        sys.exit(1)
    latest = max(cands, key=lambda p: p.stat().st_mtime)
    print(f"[INFO] Selected CSV: {latest}")
    return latest

def try_upload(g, weight: float, ts_utc: datetime) -> str:
    """여러 시그니처를 시도하고 성공 시 사용 패턴 문자열을 반환."""
    iso_ms = ts_utc.isoformat(timespec="milliseconds")
    date_s = ts_utc.astimezone(LOCAL_TZ).strftime("%Y-%m-%d")
    time_s = ts_utc.astimezone(LOCAL_TZ).strftime("%H:%M:%S")

    patterns = []

    if hasattr(g, "add_weigh_in_with_timestamps"):
        try:
            print("[DEBUG] sig add_weigh_in_with_timestamps:", inspect.signature(g.add_weigh_in_with_
