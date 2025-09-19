#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import math
import pandas as pd
from datetime import datetime, timezone
from typing import Optional, Tuple

from garth import Client as GarthClient

# Garmin Connect API
WEIGHT_POST_PATH = "/userprofile-service/userprofile/weight"

def load_csv_latest() -> pd.DataFrame:
    csv_files = [f for f in os.listdir(".") if f.lower().endswith(".csv")]
    if not csv_files:
        print("[ERROR] CSV 파일이 없습니다.")
        sys.exit(1)

    latest_csv = max(csv_files, key=os.path.getmtime)
    print(f"[INFO] Selected CSV: {latest_csv}")

    # 인코딩/구분자 자동 추정 보완 (기본은 ,)
    df = pd.read_csv(latest_csv)
    print(f"[INFO] Columns: {list(df.columns)}")
    return df

def get_env_cred() -> Tuple[str, str]:
    email = os.environ.get("GARMIN_EMAIL") or ""
    password = os.environ.get("GARMIN_PASSWORD") or ""
    if not email or not password:
        print("[ERROR] GARMIN_EMAIL / GARMIN_PASSWORD 환경변수 필요")
        sys.exit(1)
    return email, password

def login_garth(email: str, password: str) -> GarthClient:
    client = GarthClient()
    # 이메일/비밀번호 로그인
    client.login(email, password)
    # 토큰 확보 (필요 시, 최신 garth는 login에 포함되지만 안전하게 한 번 더 보장)
    try:
        client.authenticate()
    except Exception:
        pass
    print("[INFO] Garmin login OK (garth)")
    return client

def parse_row(row) -> Optional[Tuple[datetime, float]]:
    """CSV 한 줄에서 (UTC datetime, kg) 추출. 잘못된 값은 None 반환."""

    # 컬럼명(한국어) 기준
    date_raw = str(row.get("날짜", "")).strip()
    time_raw = str(row.get("시간", "")).strip()
    weight_raw = row.get("몸무게", None)

    if not date_raw or not time_raw:
        return None

    # 몸무게 NaN/0 방지
    try:
        w = float(weight_raw)
        if math.isnan(w) or w <= 0:
            return None
    except Exception:
        return None

    # 날짜는 "YYYY.MM.DD ..." 형태가 들어올 수 있으므로 앞부분만 사용
    date_part = date_raw.split()[0]
    # 시간은 HH:MM:SS 기대
    time_part = time_raw.split()[0]

    # "YYYY.MM.DD HH:MM:SS" 파싱 → UTC
    try:
        dt = datetime.strptime(f"{date_part} {time_part}", "%Y.%m.%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except ValueError:
        # 혹시 "YYYY-MM-DD" 같은 변형이 오면 한 번 더 시도
        try:
            dt = datetime.strptime(f"{date_part} {time_part}", "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        except Exception:
            return None

    return dt, w

def post_weight(client: GarthClient, when_utc: datetime, kg: float) -> None:
    """garth.connectapi 로 무게 업로드. 에러면 예외 발생."""
    epoch_ms = int(when_utc.timestamp() * 1000)

    payload = {
        # 일부 앱은 date/gmtTimestamp 둘 다 요구. 둘 다 채워 안정성 확보.
        "date": when_utc.strftime("%Y-%m-%d"),
        "gmtTimestamp": epoch_ms,
        "value": float(kg),
        "unitKey": "kg",
    }

    # garth는 base 경로/헤더/쿠키를 알아서 붙여줌
    # data=payload 로 보내면 JSON으로 직렬화되어 전송됩니다.
    resp = client.connectapi(WEIGHT_POST_PATH, data=payload, method="POST")

    # resp 가 dict 또는 requests.Response 유사 객체일 수 있음. 실패 시 garth가 예외를 던지는 편이므로
    # 여기서는 별도 상태 검사 없이 넘어가되, 방어적으로 최소 확인:
    if isinstance(resp, dict) and resp.get("message") == "error":
        raise RuntimeError(f"API error: {resp}")

def main():
    df = load_csv_latest()
    email, password = get_env_cred()
    client = login_garth(email, password)

    success, failed = 0, 0
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

    print(f"[INFO] Done. s
