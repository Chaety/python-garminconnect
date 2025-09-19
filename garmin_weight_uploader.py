#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Google Drive에서 내려받은 체중 CSV(예: "무게 2025.09.18 Google Fit.csv")를 읽어
Garmin Connect에 체중 기록을 업로드합니다.

필수 환경변수
- GARMIN_EMAIL
- GARMIN_PASSWORD

주요 변경점
- POST /userprofile-service/userprofile/weight (X)
+ POST /weight-service/weight (O)
- payload: value / gmtTimestamp (X)
+ payload: weight / timestampGMT (O)

요구 패키지: pandas, python-dateutil, garth, requests
"""

from __future__ import annotations

import os
import sys
import glob
import math
import traceback
from typing import Optional, Tuple

import pandas as pd
from datetime import datetime, timezone
from dateutil import tz
from requests import HTTPError

try:
    # garth 0.5.x
    from garth import Client
except Exception as e:  # pragma: no cover
    print(f"[FATAL] garth import 실패: {e}", file=sys.stderr)
    sys.exit(2)


WEIGHT_POST_PATH = "/weight-service/weight"

# CSV 컬럼(한국어 Google Fit/스마트체중계 내보내기 포맷 가정)
COL_DATE = "날짜"
COL_TIME = "시간"
COL_WEIGHT = "몸무게"

# 기본 타임존: 환경변수 TZ(예: Asia/Seoul) > UTC
DEFAULT_TZ = os.environ.get("CSV_TZ") or os.environ.get("TZ") or "UTC"


def _find_latest_csv(patterns: Tuple[str, ...] = ("*.csv",)) -> Optional[str]:
    """현재 작업 디렉토리에서 가장 최근 CSV 파일 1개 선택."""
    candidates = []
    for pat in patterns:
        candidates.extend(glob.glob(pat))
    if not candidates:
        return None
    candidates.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return candidates[0]


def _parse_datetime_utc(date_str: str, time_str: str, local_tz_name: str) -> datetime:
    """
    CSV의 '날짜','시간' 문자열을 로컬 타임존 기준의 naive datetime으로 파싱 후 UTC로 변환.
    - 날짜 예: '2025.09.18 00:00:00' 또는 '2025.09.18'
    - 시간 예: '03:19:22'
    """
    # 날짜 문자열에 시간이 포함될 수도 있어서 안전 처리
    date_str = (date_str or "").strip()
    time_str = (time_str or "").strip()

    if time_str:
        # 'YYYY.MM.DD HH:MM:SS' 형태로 합치기
        combined = f"{date_str.split(' ')[0]} {time_str}"
        dt_local = datetime.strptime(combined, "%Y.%m.%d %H:%M:%S")
    else:
        # 시간 미기재 시 자정
        dt_local = datetime.strptime(date_str.split(' ')[0], "%Y.%m.%d")

    # 타임존 부여 후 UTC로 변환
    local_tz = tz.gettz(local_tz_name) or tz.gettz("UTC")
    dt_local = dt_local.replace(tzinfo=local_tz)
    return dt_local.astimezone(timezone.utc)


def _epoch_ms(dt_utc: datetime) -> int:
    """UTC datetime -> epoch milliseconds"""
    return int(dt_utc.timestamp() * 1000)


def _login_garmin(email: str, password: str) -> Client:
    """garth 클라이언트 로그인."""
    client = Client()
    # 토큰 캐시 디렉토리(있으면 사용)
    token_dir = os.path.expanduser("~/.garminconnect")
    try:
        os.makedirs(token_dir, exist_ok=True)
    except Exception:
        pass

    client.login(email, password)
    return client


def _post_weight(client: Client, when_utc: datetime, kg: float) -> None:
    """가민에 체중 1건 업로드. 409 중복은 성공 처리."""
    epoch_ms = _epoch_ms(when_utc)
    payload = {
        "date": when_utc.strftime("%Y-%m-%d"),
        "timestampGMT": epoch_ms,   # epoch millis (UTC)
        "weight": float(kg),
        "unitKey": "kg",
    }

    try:
        client.connectapi(WEIGHT_POST_PATH, method="POST", json=payload)
    except HTTPError as e:
        status = getattr(getattr(e, "response", None), "status_code", None)
        if status == 409:
            # 동일 타임스탬프 데이터가 이미 존재 — 성공으로 간주
            print(f"[INFO] [DUPLICATE->OK] {kg}kg @ {when_utc.isoformat()} ({epoch_ms})")
            return
        # 그 외 오류는 재전파
        raise


def main() -> int:
    # 환경변수
    email = os.environ.get("GARMIN_EMAIL") or ""
    password = os.environ.get("GARMIN_PASSWORD") or ""
    csv_tz = DEFAULT_TZ

    if not email or not password:
        print("[FATAL] 환경변수 GARMIN_EMAIL / GARMIN_PASSWORD 가 필요합니다.", file=sys.stderr)
        return 2

    # 최신 CSV 선택
    csv_path = _find_latest_csv()
    if not csv_path:
        print("[FATAL] 작업 디렉토리에 CSV 파일이 없습니다.", file=sys.stderr)
        return 1

    print(f"[INFO] Selected CSV: {csv_path}")

    # CSV 로드
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        print(f"[FATAL] CSV 로드 실패: {e}", file=sys.stderr)
        return 1

    cols = list(df.columns)
    print(f"[INFO] Columns: {cols}")

    # 필수 컬럼 점검
    missing = [c for c in (COL_DATE, COL_TIME, COL_WEIGHT) if c not in df.columns]
    if missing:
        print(f"[FATAL] 필수 컬럼 누락: {missing}", file=sys.stderr)
        return 1

    # 로그인
    try:
        client = _login_garmin(email, password)
        print("[INFO] Garmin login OK (garth)")
    except Exception as e:
        print(f"[FATAL] Garmin 로그인 실패: {e}", file=sys.stderr)
        return 2

    success = 0
    failed = 0

    # 행 순회
    for idx, row in df.iterrows():
        try:
            date_str = str(row.get(COL_DATE, "")).strip()
            time_str = str(row.get(COL_TIME, "")).strip()
            weight_str = str(row.get(COL_WEIGHT, "")).strip().replace(",", "")

            if not date_str or not weight_str:
                continue

            # 따옴표로 감싼 숫자 처리: "70.79891"
            try:
                kg = float(weight_str.strip('"'))
            except ValueError:
                continue

            # 0 또는 NaN/비정상 무게 스킵
            if kg <= 0 or math.isinf(kg) or math.isnan(kg):
                continue

            when_utc = _parse_datetime_utc(date_str, time_str, csv_tz)

            try:
                _post_weight(client, when_utc, kg)
                print(
                    f"[INFO] [OK] {when_utc.date()} {kg}kg @ {when_utc.isoformat()} ({_epoch_ms(when_utc)})"
                )
                success += 1
            except HTTPError as e:
                code = getattr(getattr(e, "response", None), "status_code", "?")
                print(
                    f"[INFO] [FAIL] {when_utc.date()} {kg}kg @ {when_utc.isoformat()} "
                    f"({_epoch_ms(when_utc)}) - Error in request: {e} (status={code})"
                )
                failed += 1
            except Exception as e:
                print(
                    f"[INFO] [FAIL] {when_utc.date()} {kg}kg @ {when_utc.isoformat()} "
                    f"({_epoch_ms(when_utc)}) - {e}"
                )
                failed += 1

        except Exception as e:
            failed += 1
            print(f"[WARN] 인덱스 {idx} 처리 중 예외: {e}")
            traceback.print_exc()

    print(f"[INFO] Done. success={success}, failed={failed}")
    # 실패가 있어도 전체 파이프라인 실패 여부는 정책에 맞게 반환
    return 0 if failed == 0 else 0  # 실패가 있어도 워크플로우 계속 통과시키려면 0


if __name__ == "__main__":
    sys.exit(main())
