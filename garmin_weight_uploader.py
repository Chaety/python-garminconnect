#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Google Drive에서 내려받은 체중 CSV(예: "무게 2025.09.18 Google Fit.csv")를 읽어
Garmin Connect에 체중 기록을 업로드합니다.

필수 환경변수
- GARMIN_EMAIL
- GARMIN_PASSWORD

주요 사항
- 엔드포인트: POST /weight-service/weight
- 페이로드 키: weight / unitKey / timestampGMT / timestampLocal / sourceType
  * timestamp* 는 epoch milliseconds
  * sourceType 은 USER_ENTERED 로 지정
- 409(중복)은 성공으로 간주
- 400 발생 시 응답 본문도 함께 출력하도록 개선

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


# 현재 가민 Connect API 체중 업로드 경로
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


def _parse_datetime_local(date_str: str, time_str: str, local_tz_name: str) -> datetime:
    """
    CSV의 '날짜','시간' 문자열을 로컬 타임존 기준 aware datetime으로 파싱.
    - 날짜 예: '2025.09.18 00:00:00' 또는 '2025.09.18'
    - 시간 예: '03:19:22'
    """
    date_str = (date_str or "").strip()
    time_str = (time_str or "").strip()

    if time_str:
        combined = f"{date_str.split(' ')[0]} {time_str}"
        dt_naive = datetime.strptime(combined, "%Y.%m.%d %H:%M:%S")
    else:
        dt_naive = datetime.strptime(date_str.split(' ')[0], "%Y.%m.%d")

    local_tz = tz.gettz(local_tz_name) or tz.gettz("UTC")
    return dt_naive.replace(tzinfo=local_tz)


def _to_utc(dt_local: datetime) -> datetime:
    return dt_local.astimezone(timezone.utc)


def _epoch_ms(dt: datetime) -> int:
    """aware datetime -> epoch milliseconds"""
    return int(dt.timestamp() * 1000)


def _login_garmin(email: str, password: str) -> Client:
    """garth 클라이언트 로그인."""
    client = Client()
    # 토큰 캐시 디렉토리(있으면 사용)
    try:
        os.makedirs(os.path.expanduser("~/.garminconnect"), exist_ok=True)
    except Exception:
        pass
    client.login(email, password)
    return client


def _post_weight(client: Client, when_local: datetime, when_utc: datetime, kg: float) -> None:
    """가민에 체중 1건 업로드. 409 중복은 성공 처리."""
    payload = {
        # 필수값들
        "weight": float(kg),
        "unitKey": "kg",
        "timestampLocal": _epoch_ms(when_local),  # 로컬 타임존 기준 epoch ms
        "timestampGMT": _epoch_ms(when_utc),      # UTC 기준 epoch ms
        "sourceType": "USER_ENTERED",             # 수동 입력
    }

    try:
        resp = client.connectapi(WEIGHT_POST_PATH, method="POST", json=payload)
        # 일부 배포에선 201 / 200 / 204 다양하게 반환될 수 있어, 예외 없으면 성공 처리
        return
    except HTTPError as e:
        status = getattr(getattr(e, "response", None), "status_code", None)
        text = ""
        try:
            text = e.response.text[:500] if e.response is not None else ""
        except Exception:
            pass

        if status == 409:
            print(f"[INFO] [DUPLICATE->OK] {kg}kg @ {when_utc.isoformat()} {_epoch_ms(when_utc)}")
            return

        # 400 등 나머지는 에러 재전파
        raise HTTPError(f"{e} | body={text}") from e


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

            try:
                kg = float(weight_str.strip('"'))
            except ValueError:
                continue

            if kg <= 0 or math.isinf(kg) or math.isnan(kg):
                continue

            when_local = _parse_datetime_local(date_str, time_str, csv_tz)
            when_utc = _to_utc(when_local)

            try:
                _post_weight(client, when_local, when_utc, kg)
                print(
                    f"[INFO] [OK] {when_utc.date()} {kg}kg @ {when_utc.isoformat()} ({_epoch_ms(when_utc)})"
                )
                success += 1
            except HTTPError as e:
                # 여기서 status/body 함께 출력함
                print(
                    f"[INFO] [FAIL] {when_utc.date()} {kg}kg @ {when_utc.isoformat()} "
                    f"({_epoch_ms(when_utc)}) - Error in request: {e}"
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
            print(f".[WARN] 인덱스 {idx} 처리 중 예외: {e}")
            traceback.print_exc()

    print(f"[INFO] Done. success={success}, failed={failed}")
    # 실패가 있어도 CI 전체 실패로 보지 않도록 0 반환(원하면 1로 바꾸세요)
    return 0


if __name__ == "__main__":
    sys.exit(main())
