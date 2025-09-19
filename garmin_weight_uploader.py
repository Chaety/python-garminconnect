# -*- coding: utf-8 -*-
"""
Garmin Weight Uploader (Plan A: GitHub Actions-friendly)
- CSV를 리포에 커밋해 두고 이를 읽어 가민 커넥트에 체중을 업로드합니다.
- '날짜' + '시간'을 합쳐 (KST) 타임스탬프 생성 → UTC로 변환 후 업로드.
- 필수: 몸무게(kg), 날짜, 시간
- 선택: 체지방률(%) (없어도 됨)

로컬 실행:
    pip install -r requirements.txt
    export GARMIN_EMAIL="your@email"
    export GARMIN_PASSWORD="your_password"
    python garmin_weight_uploader.py

GitHub Actions:
    - 리포 시크릿에 GARMIN_EMAIL / GARMIN_PASSWORD 등록
    - .github/workflows/garmin-weight.yml 로 스케줄 실행

환경변수:
    DRY_RUN=1  → 업로드 대신 미리보기만 출력
"""

import os
import sys
import pandas as pd
from datetime import timezone, timedelta
from dateutil import parser as dateparser

# ===== CSV 파일명 (리포 루트에 커밋) =========================================
CSV_FILE = "무게 2025.09.18 Google Fit.csv"

# ===== 컬럼명 매핑 (CSV에 존재하는 정확한 이름으로 설정) ======================
DATE_COL   = "날짜"     # 예: 2025-09-18
TIME_COL   = "시간"     # 예: 21:03:00  (비어있으면 날짜만으로 처리)
WEIGHT_COL = "몸무게"   # kg (필수)
FAT_COL    = "체지방률"  # % (선택, 없으면 자동 무시)

# ===== 타임존: KST(UTC+9) ===================================================
LOCAL_TZ = timezone(timedelta(hours=9))  # Asia/Seoul

def to_float(val):
    """문자열/퍼센트 포함 값들을 float으로 안전 변환"""
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
    """DATE_COL + TIME_COL 결합하여 KST 기준 → UTC 변환"""
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

def read_csv_safely(path):
    """UTF-8-SIG → UTF-8 → CP949 순서로 시도"""
    last_err = None
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception as e:
            last_err = e
    print("[ERROR] CSV 읽기 실패. 마지막 에러:", last_err)
    sys.exit(1)

def ensure_deps():
    for dep in ("garminconnect", "pytz", "dateutil", "pandas"):
        try:
            __import__(dep)
        except ImportError:
            print(f"[ERROR] {dep} 미설치. 다음으로 설치하세요: pip install -r requirements.txt")
            sys.exit(1)

def main():
    ensure_deps()
    from garminconnect import Garmin

    email = os.getenv("GARMIN_EMAIL")
    password = os.getenv("GARMIN_PASSWORD")
    if not email or not password:
        print("[WARN] GARMIN_EMAIL / GARMIN_PASSWORD 미설정. 최초 로그인 시 프롬프트가 뜰 수 있습니다(토큰 캐시됨).")

    dry_run = os.getenv("DRY_RUN", "0") == "1"

    df = read_csv_safely(CSV_FILE)
    # 필수 컬럼 체크
    for col in (DATE_COL, WEIGHT_COL):
        if col not in df.columns:
            print(f"[ERROR] 필수 컬럼 누락: {col} / CSV 보유 컬럼: {list(df.columns)}")
            sys.exit(1)
    # TIME_COL은 없어도 동작 가능

    # 가민 로그인 (토큰은 ~/.garminconnect에 캐시)
    g = Garmin(email, password)
    g.login()

    success = 0
    failed = 0

    for idx, row in df.iterrows():
        date_val = row.get(DATE_COL, None)
        time_val = row.get(TIME_COL, None) if TIME_COL in df.columns else None

        ts_utc = parse_ts(date_val, time_val)
        weight_kg = to_float(row.get(WEIGHT_COL, None))
        percent_fat = to_float(row.get(FAT_COL, None)) if (FAT_COL and FAT_COL in df.columns) else None

        if ts_utc is None or weight_kg is None:
            print(f"[SKIP] Row {idx}: timestamp/weight 누락. date={date_val}, time={time_val}, weight={row.get(WEIGHT_COL)}")
            failed += 1
            continue

        if dry_run:
            print(f"[DRY] {idx}: ts={ts_utc.isoformat()}, weight={weight_kg}, fat={percent_fat}")
            success += 1
            continue

        try:
            # 타임스탬프 지정 업로드가 가능한 버전이면 우선 사용
            if hasattr(g, "add_weigh_in_with_timestamps"):
                g.add_weigh_in_with_timestamps(
                    weight_kg=weight_kg,
                    percent_fat=percent_fat,
                    bmi=None,
                    timestamp=ts_utc.isoformat(timespec="milliseconds")
                )
            else:
                # 구버전 fallback: timestamp가 무시될 수 있음
                g.add_weigh_in(weight_kg=weight_kg, percent_fat=percent_fat, bmi=None)
            print(f"[OK] Row {idx}: {ts_utc.isoformat()} UTC, weight={weight_kg}, fat={percent_fat}")
            success += 1
        except Exception as e:
            print(f"[FAIL] Row {idx}: {e}")
            failed += 1

    print(f"Done. success={success}, failed={failed}")

if __name__ == "__main__":
    main()
