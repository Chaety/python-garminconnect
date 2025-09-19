#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import math
import pandas as pd
from datetime import datetime, timezone
from typing import Optional, Tuple

from garth import Client as GarthClient

# Garmin Connect API endpoint for weight entries
WEIGHT_POST_PATH = "/userprofile-service/userprofile/weight"

def load_csv_latest() -> pd.DataFrame:
    """가장 최근 CSV를 읽어 DataFrame 반환"""
    csv_files = [f for f in os.listdir(".") if f.lower().endswith(".csv")]
    if not csv_files:
        print("[ERROR] CSV 파일이 없습니다.")
        sys.exit(1)

    latest_csv = max(csv_files, key=os.path.getmtime)
    print(f"[INFO] Selected CSV: {latest_csv}")

    # 기본은 쉼표 구분/UTF-8 가정. 필요시 여기서 encoding이나 sep 조정 가능.
    df = pd.read_csv(latest_csv)
    print(f"[INFO] Columns: {list(df.columns)}")
    return df

def get_env_cred() -> Tuple[str, str]:
    """환경변수에서 Garmin 계정 정보 읽기"""
    email = os.environ.get("GARMIN_EMAIL") or ""
    password = os.environ.get("GARMIN_PASSWORD") or ""
    if not email or not password:
        print("[ERROR] GARMIN_EMAIL / GARMIN_PASSWORD 환경변수 필요")
        sys.exit(1)
    return email, password

def login_garth(email: str, password: str) -> GarthClient:
    """garth 클라이언트 로그인"""
    client = GarthClient()
    client.login(email, password)
    # 일부 버전은 authenticate가 내부에서 실행되므로 예외 무시하고 보조 호출
    try:
        client.authenticate()
    except Exception:
        pass
    print("[INFO] Garmin login OK (garth)")
    return client

def parse_row(row) -> Optional[Tuple[datetime, float]]:
    """
    CSV 한 줄에서 (UTC datetime, kg) 추출.
    형식: 날짜(YYYY.MM.DD ...), 시간(HH:MM:SS), 몸무게
    잘못된 값은 None 반환.
    """
    date_raw = str(row.get("날짜", "")).strip()
    time_raw = str(row.get("시간", "")).strip()
    weight_raw = row.get("몸무게", None)

    if not date_raw or not time_raw:
        return None

    # 몸무게 NaN/0/음수/비수치 방지
    try:
        w = float(weight_raw)
        if math.isnan(w) or w <= 0:
            return None
    except Exception:
        return None

    # 날짜/시간 앞부분만 사용
    date_part = date_raw.split()[0]
    time_part = time_raw.split()[0]

    # UTC로 간주해 업로드 (원 데이터가 현지시라면 필요시 tz 변환 추가)
    # 기본 포맷 시도: 2025.09.18 03:19:22
    dt: Optional[datetime] = None
    try:
        dt = datetime.strptime(f"{date_part} {time_part}", "%Y.%m.%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except ValueError:
        # 보조 포맷: 2025-09-18 03:19:22
        try:
            dt = datetime.strptime(f"{date_part} {time_part}", "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        except Exception:
            return None

    return dt, w

def post_weight(client: GarthClient, when_utc: datetime, kg: float) -> None:
    """garth.connectapi 로 무게 업로드. 실패 시 예외."""
    epoch_ms = int(when_utc.timestamp() * 1000)

    payload = {
        # 안정성을 위해 날짜 문자열과 GMT epoch(ms) 모두 포함
        "date": when_utc.strftime("%Y-%m-%d"),
        "gmtTimestamp": epoch_ms,
        "value": float(kg),
        "unitKey": "kg",
    }

    # garth가 세션/헤더를 관리하므로 경로와 메서드/바디만 지정
    # data= 대신 json= 사용 (서버가 JSON 기대)
    resp = client.connectapi(WEIGHT_POST_PATH, json=payload, method="POST")

    # 실패 시 garth가 예외를 던지는 편이지만, dict 응답 형식 방어적 확인
    if isinstance(resp, dict) and resp.get("message") == "error":
        raise RuntimeError(f"API error: {resp}")

def main():
    df = load_csv_latest()
    email, password = get_env_cred()
    client = login_garth(email, password)

    success, failed = 0, 0

    # 행 순회
    for _, row in df.iterrows():
        parsed = parse_row(row)
        if not parsed:
            continue

        dt_utc, kg = parsed
        epoch_ms = int(dt_utc.timestamp() * 1000)

        try:
            post_weight(client, dt_utc, kg)
            print(f"[INFO] [OK] {dt_utc.date()} {kg}kg @ {dt_utc.isoformat()} ({epoch_ms})")
            success += 1
        except Exception as e:
            print(f"[INFO] [FAIL] {dt_utc.date()} {kg}kg @ {dt_utc.isoformat()} ({epoch_ms}) - {e}")
            failed += 1

    print(f"[INFO] Done. success={success}, failed={failed}")

if __name__ == "__main__":
    main()
