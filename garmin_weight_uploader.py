# -*- coding: utf-8 -*-
"""
Garmin Weight Uploader
- 작업 디렉토리의 *.csv 중 가장 최신 파일 자동 선택
- '날짜' + '시간' → UTC timestamp 생성 후 가민 커넥트에 업로드
- 필수 컬럼: 날짜, 몸무게
- 선택 컬럼: 시간
"""

import os
import sys
import pandas as pd
from datetime import timezone, timedelta
from dateutil import parser as dateparser
from pathlib import Path

DATE_COL   = "날짜"
TIME_COL   = "시간"
WEIGHT_COL = "몸무게"

LOCAL_TZ = timezone(timedelta(hours=9))  # KST

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
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception:
            continue
    print("[ERROR] CSV 읽기 실패:", path)
    sys.exit(1)

def pick_latest_csv() -> Path:
    candidates = sorted(Path(".").glob("*.*
