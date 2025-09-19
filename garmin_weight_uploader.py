#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Google Drive에서 내려받은 체중 CSV(예: "무게 2025.09.18 Google Fit.csv")를 읽어
Garmin Connect에 체중 기록을 업로드합니다.

필수 환경변수
- GARMIN_EMAIL
- GARMIN_PASSWORD

선택 환경변수
- CSV_TZ (기본: Asia/Seoul)  # CSV의 '날짜/시간'이 기록된 로컬 타임존

주요 사항
- 엔드포인트: POST /weight-service/weight
- 페이로드 키: weight / unitKey / timestampGMT / timestampLocal / sourceType
- weight는 소수점 2자리로 반올림
- timestampLocal/ timestampGMT 는 서로 타임존 오프셋만큼 차이나야 함(미스매치 시 400 가능)
- 409(중복)은 성공으로 간주
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
    from garth import Client
except Exception as e:
    print(f"[FATAL] garth import 실패: {e}", file=sys.stderr)
    sys.exit(2)

WEIGHT_POST_PATH = "/weight-service/weight"

COL_DATE = "날짜"
COL_TIME = "시간"
COL_WEIGHT = "몸무게"

# 기본 타임존: 한국 데이터 가정 (필요 시 CSV_TZ 로 변경)
DEFAULT_TZ = os.environ.get("CSV_TZ") or "Asia/Seoul"


def _find_latest_csv(patterns: Tuple[str, ...] = ("*.csv",)) -> Optional[str]:
    files = []
    for pat in patterns:
        files.extend(glob.glob(pat))
    if not files:
        return None
    files.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return files[0]


def _parse_datetime_local(date_str: str, time_str: str, local_tz_name: str) -> datetime:
    date_str = (date_str or "").strip()
    time_str = (time_str or "").strip()

    # 일부 내보내기 포맷이 'YYYY.MM.DD HH:MM:SS' 형태로 날짜 열에 시간까지 들어있는 경우가 있어 split 처리
    base_date = date_str.split(" ")[0]

    if time_str:
        combined = f"{base_date} {time_str}"
        dt_naive = datetime.strptime(combined, "%Y.%m.%d %H:%M:%S")
    else:
        dt_naive = datetime.strptime(base_date, "%Y.%m.%d")

    local_tz = tz.gettz(local_tz_name) or tz.gettz("Asia/Seoul") or tz.UTC
    return dt_naive.replace(tzinfo=local_tz)


def _to_utc(dt_local: datetime) -> datetime:
    return dt_local.astimezone(timezone.utc)


def _epoch_ms(dt: datetime) -> int:
    return int(round(dt.timestamp() * 1000))


def _login_garmin(email: str, password: str) -> Client:
    client = Client()
    try:
        os.makedirs(os.path.expanduser("~/.garminconnect"), exist_ok=True)
    except Exception:
        pass
    client.login(email, password)
    return client


def _post_weight(client: Client, when_local: datetime, when_utc: datetime, kg: float) -> None:
    # 가민이 소수점 2자리까지 허용하는 배포가 있어 반올림
    kg2 = round(float(kg), 2)

    payload = {
        "weight": kg2,
        "unitKey": "kg",
        "timestampLocal": _epoch_ms(when_local),  # 로컬 기준 epoch ms
        "timestampGMT": _epoch_ms(when_utc),      # UTC 기준 epoch ms
        "sourceType": "USER_ENTERED",
    }

    try:
        client.connectapi(WEIGHT_POST_PATH, method="POST", json=payload)
        return
    except HTTPError as e:
        # 상태/본문 최대한 자세히 출력
        status = getattr(getattr(e, "response", None), "status_code", None)
        body = ""
        try:
            if e.response is not None:
                # 본문이 JSON일 수도, 텍스트일 수도 있음
                ctype = e.response.headers.get("Content-Type", "")
                if "application/json" in ctype:
                    body = e.response.text
                else:
                    body = e.response.text
                body = (body or "")[:1000]
        except Exception:
            pass

        if status == 409:
            print(f"[INFO] [DUPLICATE->OK] {kg2}kg @ {when_utc.isoformat()} {_epoch_ms(when_utc)}")
            return

        # 디버깅을 위해 상태/본문을 포함해 예외 재던짐
        raise HTTPError(f"{status} Error | body={body}") from e


def main() -> int:
    email = os.environ.get("GARMIN_EMAIL") or ""
    password = os.environ.get("GARMIN_PASSWORD") or ""
    csv_tz = DEFAULT_TZ

    if not email or not password:
        print("[FATAL] 환경변수 GARMIN_EMAIL / GARMIN_PASSWORD 가 필요합니다.", file=sys.stderr)
        return 2

    csv_path = _find_latest_csv()
    if not csv_path:
        print("[FATAL] 작업 디렉토리에 CSV 파일이 없습니다.", file=sys.stderr)
        return 1

    print(f"[INFO] Selected CSV: {csv_path}")

    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        print(f"[FATAL] CSV 로드 실패: {e}", file=sys.stderr)
        return 1

    print(f"[INFO] Columns: {list(df.columns)}")
    missing = [c for c in (COL_DATE, COL_TIME, COL_WEIGHT) if c not in df.columns]
    if missing:
        print(f"[FATAL] 필수 컬럼 누락: {missing}", file=sys.stderr)
        return 1

    try:
        client = _login_garmin(email, password)
        print("[INFO] Garmin login OK (garth)")
    except Exception as e:
        print(f"[FATAL] Garmin 로그인 실패: {e}", file=sys.stderr)
        return 2

    success = 0
    failed = 0

    for idx, row in df.iterrows():
        try:
            date_str = str(row.get(COL_DATE, "")).strip()
            time_str = str(row.get(COL_TIME, "")).strip()
            weight_str = str(row.get(COL_WEIGHT, "")).strip().replace(",", "")

            if not date_str or not weight_str:
                continue

            try:
                kg_raw = float(weight_str.strip('"'))
            except ValueError:
                continue

            if kg_raw <= 0 or math.isinf(kg_raw) or math.isnan(kg_raw):
                continue

            when_local = _parse_datetime_local(date_str, time_str, csv_tz)
            when_utc = _to_utc(when_local)

            try:
                _post_weight(client, when_local, when_utc, kg_raw)
                print(f"[INFO] [OK] {when_utc.date()} {round(kg_raw,2)}kg @ {when_utc.isoformat()} ({_epoch_ms(when_utc)})")
                success += 1
            except HTTPError as e:
                print(
                    f"[INFO] [FAIL] {when_utc.date()} {round(kg_raw,2)}kg @ {when_utc.isoformat()} "
                    f"({_epoch_ms(when_utc)}) - {e}"
                )
                failed += 1
            except Exception as e:
                print(
                    f"[INFO] [FAIL] {when_utc.date()} {round(kg_raw,2)}kg @ {when_utc.isoformat()} "
                    f"({_epoch_ms(when_utc)}) - {e}"
                )
                failed += 1

        except Exception as e:
            failed += 1
            print(f".[WARN] 인덱스 {idx} 처리 중 예외: {e}")
            traceback.print_exc()

    print(f"[INFO] Done. success={success}, failed={failed}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
