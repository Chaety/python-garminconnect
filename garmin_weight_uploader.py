# -*- coding: utf-8 -*-
"""
Garmin Weight Uploader (Google Drive → GitHub Actions)
- 실행 시 작업 디렉토리에 있는 "무게 *.csv" 중 가장 최신 파일을 자동 선택
- '날짜' + '시간'을 KST로 합쳐 UTC 타임스탬프 생성 후 업로드
- 필수 컬럼: 날짜, 시간, 몸무게
- 선택 컬럼: 체지방률 (없어도 됨)

환경변수:
  GARMIN_EMAIL / GARMIN_PASSWORD : 가민 계정
  DRY_RUN=1  → 업로드 대신 미리보기만 출력
"""

import os
import sys
import glob
import pandas as pd
from datetime import timezone, timedelta
from dateutil import parser as dateparser
from pathlib import Path

# ===== 컬럼명 (CSV에 존재하는 정확한 이름으로 맞춤) ==========================
DATE_COL   = "날짜"     # 예: 2025-09-18
TIME_COL   = "시간"     # 예: 21:03:00 (비어있으면 날짜만으로 처리)
WEIGHT_COL = "몸무게"   # kg
FAT_COL    = "체지방률"  # % (선택)

# ===== 타임존: KST(UTC+9) ===================================================
LOCAL_TZ = timezone(timedelta(hours=9))  # Asia/Seoul

def to_float(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        s = str(val).strip().replace(",", "").replace("%", "")
        if s == "":
            return None
        return float(s)
    except Exception:
        return None

def parse_ts(date_val, time_val):
    if pd.isna(date_val) or str(date_val).strip() == "":
        return None
    ts_text = str(date_val) if not time_val or (isinstance(time_val, float) and pd.isna(time_val)) or str(time_val).strip()=="" \
              else f"{date_val} {time_val}"
    dt = dateparser.parse(ts_text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=LOCAL_TZ)
    return dt.astimezone(timezone.utc)

def read_csv_safely(path):
    last_err = None
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception as e:
            last_err = e
    print("[ERROR] CSV 읽기 실패. 마지막 에러:", last_err)
    sys.exit(1)

def pick_latest_csv():
    # 리포 workspace(현재 작업 디렉토리)에서 "무게 *.csv" 목록 중 최신 파일 선택
    candidates = sorted(Path(".").glob("무게 *.csv"))
    if not candidates:
        print("[ERROR] 작업 디렉토리에 '무게 *.csv' 파일이 없습니다. 드라이브 다운로드 스텝을 확인하세요.")
        sys.exit(1)
    # 수정시각 기준 최신
    latest = max(candidates, key=lambda p: p.stat().st_mtime)
    print(f"[INFO] 선택된 CSV: {latest}")
    return str(latest)

def ensure_deps():
    for dep in ("garminconnect", "pytz", "dateutil", "pandas"):
        try:
            __import__(dep)
        except ImportError:
            print(f"[ERROR] {dep} 미설치. pip install -r requirements.txt 로 설치하세요.")
            sys.exit(1)

def main():
    ensure_deps()
    from garminconnect import Garmin

    email = os.getenv("GARMIN_EMAIL")
    password = os.getenv("GARMIN_PASSWORD")
    if not email or not password:
        print("[WARN] GARMIN_EMAIL / GARMIN_PASSWORD 미설정. 최초 로그인 시 프롬프트가 뜰 수 있음(토큰 캐시됨).")

    dry_run = os.getenv("DRY_RUN", "0") == "1"

    csv_file = pick_latest_csv()
    df = read_csv_safely(csv_file)

    # 필수 컬럼 체크
    for col in (DATE_COL, WEIGHT_COL):
        if col not in df.columns:
            print(f"[ERROR] 필수 컬럼 누락: {col} / CSV 보유 컬럼: {list(df.columns)}")
            sys.exit(1)
    # TIME_COL은 없어도 동작 가능

    g = Garmin(email, password)
    g.login()  # 토큰은 ~/.garminconnect 에 캐시

    success = 0
    failed = 0

    for idx, row in df.iterrows():
        date_val = row.get(DATE_COL)
        time_val = row.get(TIME_COL) if TIME_COL in df.columns else None
        ts_utc = parse_ts(date_val, time_val)
        weight_kg = to_float(row.get(WEIGHT_COL))
        percent_fat = to_float(row.get(FAT_COL)) if (FAT_COL and FAT_COL in df.columns) else None

        if ts_utc is None or weight_kg is None:
            print(f"[SKIP] Row {idx}: timestamp/weight 누락. date={date_val}, time={time_val}, weight={row.get(WEIGHT_COL)}")
            failed += 1
            continue

        if dry_run:
            print(f"[DRY] {idx}: ts={ts_utc.isoformat()}, weight={weight_kg}, fat={percent_fat}")
            success += 1
            continue

        try:
            if hasattr(g, "add_weigh_in_with_timestamps"):
                g.add_weigh_in_with_timestamps(
                    weight_kg=weight_kg,
                    percent_fat=percent_fat,
                    bmi=None,
                    timestamp=ts_utc.isoformat(timespec="milliseconds")
                )
            else:
                g.add_weigh_in(weight_kg=weight_kg, percent_fat=percent_fat, bmi=None)
            print(f"[OK] Row {idx}: {ts_utc.isoformat()} UTC, weight={weight_kg}, fat={percent_fat}")
            success += 1
        except Exception as e:
            print(f"[FAIL] Row {idx}: {e}")
            failed += 1

    print(f"Done. success={success}, failed={failed}")

if __name__ == "__main__":
    main()
