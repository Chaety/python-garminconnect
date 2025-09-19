#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Garmin Connect에 체중 기록을 업로드하는 스크립트 (CSV -> Garmin)
- 타임스탬프: epoch "초" 단위 전송
- sourceType: "MANUAL"
- 409(중복)은 성공으로 간주
- 400/기타 오류 시 응답 본문을 디버그 출력
환경변수:
  GARMIN_EMAIL, GARMIN_PASSWORD (필수)
  TZ (선택, 기본 "Asia/Seoul")
CSV 컬럼(예시):
  날짜,시간,몸무게,체지방률,... (샘플에서 '날짜'는 "YYYY.MM.DD HH:MM:SS" 형태)
"""

from __future__ import annotations

import os
import sys
import glob
import math
from dataclasses import dataclass
from typing import List, Optional, Tuple

import pandas as pd
from datetime import datetime
from dateutil import tz
from requests import HTTPError

# garth 0.5.x
try:
    from garth import Client  # type: ignore
except Exception:
    Client = None  # type: ignore

WEIGHT_POST_PATH = "/weight-service/weight"

K_COL_DATE = "날짜"
K_COL_TIME = "시간"
K_COL_WEIGHT = "몸무게"

DEFAULT_TZ = os.getenv("TZ", "Asia/Seoul")


@dataclass
class WeightRow:
    when_local: datetime
    when_utc: datetime
    weight_kg: float


def _die(msg: str) -> None:
    print(f"[ERROR] {msg}", file=sys.stderr)
    sys.exit(1)


def _epoch_s(dt: datetime) -> int:
    # epoch seconds (10 digits)
    return int(round(dt.timestamp()))


def _find_latest_csv() -> Optional[str]:
    files = [f for f in glob.glob("*.csv") if os.path.isfile(f)]
    if not files:
        return None
    files.sort(key=lambda f: os.path.getmtime(f), reverse=True)
    return files[0]


def _parse_kr_datetime(date_str: str, time_str: Optional[str], tzname: str) -> datetime:
    """
    샘플 CSV의 '날짜'는 'YYYY.MM.DD HH:MM:SS' 형태로 이미 시간까지 포함합니다.
    일부 시트는 '시간'이 별도로 존재할 수 있어 보조적으로 사용합니다.
    우선 '날짜'를 우선 파싱하고, 실패하면 '날짜+시간' 조합을 사용합니다.
    """
    tz_local = tz.gettz(tzname)

    # 1) '날짜' 자체에 시간까지 포함돼 있을 때
    for fmt in ("%Y.%m.%d %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(date_str.strip(), fmt)
            return dt.replace(tzinfo=tz_local)
        except ValueError:
            pass

    # 2) '날짜'가 'YYYY.MM.DD' 이고 '시간'이 HH:MM:SS로 따로 있을 때
    if time_str:
        for dfmt in ("%Y.%m.%d", "%Y-%m-%d"):
            for tfmt in ("%H:%M:%S", "%H:%M"):
                try:
                    d = datetime.strptime(date_str.strip(), dfmt)
                    t = datetime.strptime(time_str.strip(), tfmt).time()
                    dt = datetime.combine(d.date(), t).replace(tzinfo=tz_local)
                    return dt
                except ValueError:
                    continue

    # 3) 마지막 시도: 날짜만 처리하여 자정 처리
    for dfmt in ("%Y.%m.%d", "%Y-%m-%d"):
        try:
            d = datetime.strptime(date_str.strip(), dfmt)
            dt = datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=tz_local)
            return dt
        except ValueError:
            pass

    raise ValueError(f"지원하지 않는 날짜/시간 형식: date='{date_str}', time='{time_str}'")


def _to_utc(dt_local: datetime) -> datetime:
    return dt_local.astimezone(tz.UTC)


def _clean_weight(val) -> Optional[float]:
    try:
        f = float(val)
        if math.isnan(f):
            return None
        # 0이거나 비정상 값 무시
        if f <= 0:
            return None
        # 가민이 허용하는 범위 내에서 소수 2자리
        return round(f, 2)
    except Exception:
        return None


def _load_rows_from_csv(csv_path: str, tzname: str) -> List[WeightRow]:
    df = pd.read_csv(csv_path, dtype=str).fillna("")

    # 필수 컬럼 확인
    cols = list(df.columns)
    print(f"[INFO] Selected CSV: {os.path.basename(csv_path)}")
    print(f"[INFO] Columns: {cols}")

    if K_COL_DATE not in df.columns or K_COL_WEIGHT not in df.columns:
        _die(f"CSV에 필요한 컬럼이 없습니다. 필요한 컬럼: '{K_COL_DATE}', '{K_COL_WEIGHT}'")

    rows: List[WeightRow] = []
    for _, r in df.iterrows():
        date_str = str(r.get(K_COL_DATE, "")).strip()
        time_str = str(r.get(K_COL_TIME, "")).strip() if K_COL_TIME in df.columns else None
        weight_raw = r.get(K_COL_WEIGHT, "")

        w = _clean_weight(weight_raw)
        if not w:
            continue

        try:
            when_local = _parse_kr_datetime(date_str, time_str, tzname)
            when_utc = _to_utc(when_local)
        except Exception as e:
            print(f"[WARN] 날짜 파싱 실패: {date_str} {time_str} -> {e}")
            continue

        rows.append(WeightRow(when_local=when_local, when_utc=when_utc, weight_kg=w))

    return rows


def _login_garmin(email: str, password: str) -> "Client":
    if Client is None:
        _die("garth.Client 임포트 실패. garth가 설치되어 있는지 확인하세요.")
    client = Client()
    token_dir = os.path.expanduser("~/.garminconnect")
    try:
        os.makedirs(token_dir, exist_ok=True)
    except Exception:
        pass

    # 토큰 캐시 사용
    token_path = os.path.join(token_dir, "tokens")
    try:
        client.restore(token_path)
    except Exception:
        pass

    if not client.is_logged_in():
        client.login(email=email, password=password)
        try:
            client.dump(token_path)
        except Exception:
            pass

    return client


def _post_weight(client: "Client", when_local: datetime, when_utc: datetime, kg: float) -> None:
    payload = {
        "weight": kg,                 # kg
        "unitKey": "kg",
        "timestampLocal": _epoch_s(when_local),
        "timestampGMT": _epoch_s(when_utc),
        "sourceType": "MANUAL",
    }

    try:
        client.connectapi(WEIGHT_POST_PATH, method="POST", json=payload)
        return
    except HTTPError as e:
        status = getattr(getattr(e, "response", None), "status_code", None)
        # 409는 동일 타임스탬프/값 중복
        if status == 409:
            print(f"[INFO] [DUPLICATE->OK] {kg}kg @ {when_utc.isoformat()}")
            return

        # 디버그용 응답 본문 최대 1500자 출력
        text = ""
        try:
            if e.response is not None:
                text = (e.response.text or "")[:1500]
        except Exception:
            pass

        print(f"[DEBUG] request payload => {payload}")
        if text:
            print(f"[DEBUG] response body  => {text}")
        raise


def main() -> None:
    email = os.getenv("GARMIN_EMAIL", "").strip()
    password = os.getenv("GARMIN_PASSWORD", "").strip()
    if not email or not password:
        _die("GARMIN_EMAIL / GARMIN_PASSWORD 환경변수를 설정하세요.")

    # 최신 CSV 고르기
    csv_path = _find_latest_csv()
    if not csv_path:
        _die("작업 디렉토리에 CSV 파일이 없습니다. (예: '무게 ... .csv')")

    rows = _load_rows_from_csv(csv_path, DEFAULT_TZ)
    if not rows:
        _die("업로드할 유효한 레코드가 없습니다. (몸무게 0/빈값 제외됨)")

    # 로그인
    client = _login_garmin(email, password)
    print("[INFO] Garmin login OK (garth)")

    success = 0
    failed = 0

    for row in rows:
        try:
            _post_weight(client, row.when_local, row.when_utc, row.weight_kg)
            print(f"[INFO] [OK] {row.when_utc.date()} {row.weight_kg}kg @ {row.when_utc.isoformat()} ({_epoch_s(row.when_utc)})")
            success += 1
        except Exception as e:
            print(
                f"[INFO] [FAIL] {row.when_utc.date()} {row.weight_kg}kg @ "
                f"{row.when_utc.isoformat()} ({_epoch_s(row.when_utc)}) - {e}"
            )
            failed += 1

    print(f"[INFO] Done. success={success}, failed={failed}")


if __name__ == "__main__":
    main()
