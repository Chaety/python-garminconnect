# -*- coding: utf-8 -*-
"""
Garmin Weight Uploader (auto-detect timestamp support)
- 작업 디렉토리의 *.csv 중 최신 파일을 선택
- '날짜' + '시간' (KST) → UTC 변환
- 가민 라이브러리의 버전에 따라 가능한 timestamp 전달 방법을 자동 탐색
"""

import os
import sys
import inspect
from pathlib import Path
from datetime import timezone, timedelta, datetime

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
    """
    라이브러리 버전에 따라 다양한 인자 조합을 시도하고,
    성공 시 사용한 패턴 문자열을 반환.
    """
    iso_ms = ts_utc.isoformat(timespec="milliseconds")
    date_str = ts_utc.astimezone(LOCAL_TZ).strftime("%Y-%m-%d")
    time_str = ts_utc.astimezone(LOCAL_TZ).strftime("%H:%M:%S")

    # 0) 메서드 존재 여부 및 시그니처 로깅
    patterns = []
    has_with_ts = hasattr(g, "add_weigh_in_with_timestamps")
    if has_with_ts:
        sig = None
        try:
            sig = str(inspect.signature(g.add_weigh_in_with_timestamps))
        except Exception:
            pass
        print(f"[DEBUG] add_weigh_in_with_timestamps available. signature={sig}")

        # A. (weight=, timestamp=ISO)
        patterns.append(("add_weigh_in_with_timestamps", dict(weight=weight, timestamp=iso_ms)))
        # B. (weight=)만
        patterns.append(("add_weigh_in_with_timestamps", dict(weight=weight)))

    # 기본 add_weigh_in
    if hasattr(g, "add_weigh_in"):
        sig2 = None
        try:
            sig2 = str(inspect.signature(g.add_weigh_in))
        except Exception:
            pass
        print(f"[DEBUG] add_weigh_in available. signature={sig2}")

        # C. (weight=, timestamp=ISO) – 일부 포크/버전 호환
        patterns.append(("add_weigh_in", dict(weight=weight, timestamp=iso_ms)))
        # D. (weight=, date=YYYY-MM-DD, time=HH:MM:SS) – 구버전 가정
        patterns.append(("add_weigh_in", dict(weight=weight, date=date_str, time=time_str)))
        # E. (weight=, date=YYYY-MM-DD) – 구버전 가정
        patterns.append(("add_weigh_in", dict(weight=weight, date=date_str)))
        # F. (weight=)만 – 최후의 수단
        patterns.append(("add_weigh_in", dict(weight=weight)))

    # 시도
    last_err = None
    for meth, kwargs in patterns:
        try:
            print(f"[DEBUG] try {meth} with kwargs={kwargs}")
            getattr(g, meth)(**kwargs)
            return f"{meth}{kwargs}"
        except TypeError as te:
            # 시그니처 불일치 → 다음 패턴
            print(f"[DEBUG] TypeError with {meth}: {te}")
            last_err = te
            continue
        except Exception as e:
            print(f"[DEBUG] Exception with {meth}: {e}")
            last_err = e
            continue

    # 전부 실패 시 예외
    raise RuntimeError(f"all patterns failed; last_err={last_err}")

def main():
    ensure_deps()
    from garminconnect import Garmin

    email = os.getenv("GARMIN_EMAIL")
