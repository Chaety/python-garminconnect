# garmin_weight_uploader.py
# - CSV의 "날짜","시간","몸무게"(Korean headers)에서 과거 시각과 체중을 읽어
#   가민 Connect에 정확한 타임스탬프로 업로드한다.
# - 가장 성공률 높은 direct POST 방식(garth 세션 + weight-service)을 사용.
# - 204 / 201 / 200 은 모두 성공으로 처리. (가민이 본문 없이 204를 자주 반환)
# - 두 가지 페이로드 포맷을 순차 시도하여 호환성 ↑
# - 재시도(지수 백오프) 포함.

from __future__ import annotations
import os
import time
import json
import math
from dataclasses import dataclass
from typing import Optional, Tuple

import pandas as pd
from dateutil import tz
from datetime import datetime
import requests
import garth  # pip install garth


# ---------- 설정 ----------
# CSV 열 이름(한국어 Google Fit 추출 형식)
COL_DATE = "날짜"
COL_TIME = "시간"
COL_WEIGHT = "몸무게"

# CSV 파일 선택 우선순위(가장 최신 한 개)
CSV_GLOB = "*.csv"

# 로컬시간대(구글 핏 CSV가 현지시각으로 적히는 경우가 대부분)
LOCAL_TZ = os.environ.get("LOCAL_TZ", "Asia/Seoul")

# 가민 엔드포인트 (널리 사용되는 비공개 엔드포인트)
WEIGHT_POST_URL = "https://connect.garmin.com/modern/proxy/weight-service/user-weight"

# 재시도 설정
MAX_RETRIES = 4
BASE_SLEEP = 1.5  # seconds
# -------------------------


@dataclass
class UploadResult:
    ok: bool
    status: int
    text: str
    attempt: int


def _read_latest_csv() -> Tuple[str, pd.DataFrame]:
    import glob
    files = sorted(glob.glob(CSV_GLOB), key=os.path.getmtime, reverse=True)
    if not files:
        raise FileNotFoundError("CSV 파일을 찾을 수 없습니다 (*.csv). 드라이브 복사 단계 확인 필요.")
    csv = files[0]
    df = pd.read_csv(csv)
    return csv, df


def _parse_row_to_dt_kg(row) -> Tuple[datetime, float]:
    """행에서 (현지 시각 datetime, kg) 반환. 숫자/문자 형태 모두 안전 처리."""
    date_s = str(row.get(COL_DATE, "")).strip()
    time_s = str(row.get(COL_TIME, "")).strip()
    w_s = str(row.get(COL_WEIGHT, "")).strip().replace('"', '').replace(',', '')

    if not date_s:
        raise ValueError("날짜 열이 비어있습니다.")
    if not time_s:
        raise ValueError("시간 열이 비어있습니다.")
    if not w_s:
        raise ValueError("몸무게 열이 비어있습니다.")

    # 날짜/시간 파싱
    # Google Fit 예: 2025.09.18  /  23:03:00
    dt_local = datetime.strptime(f"{date_s} {time_s}", "%Y.%m.%d %H:%M:%S").replace(tzinfo=tz.gettz(LOCAL_TZ))

    # 몸무게 kg
    try:
        kg = float(w_s)
    except Exception:
        kg = float(w_s.replace("kg", "").strip())

    return dt_local, kg


def _payloads_for(dt_local: datetime, kg: float) -> Tuple[dict, dict]:
    """
    호환성 높은 두 가지 페이로드를 준비한다.
    1) timestampGMT (UTC ISO) + unitKey
    2) date + time (현지시각 분리표기)
    """
    dt_utc = dt_local.astimezone(tz.UTC)
    payload1 = {
        # 가민 쪽에서 자주 쓰이는 키
        "timestampGMT": dt_utc.strftime("%Y-%m-%dT%H:%M:%S.000"),
        "weight": kg,
        "unitKey": "kg",
        "sourceType": "MANUAL",
    }
    payload2 = {
        "date": dt_local.strftime("%Y-%m-%d"),
        "time": dt_local.strftime("%H:%M:%S"),
        "weight": kg,
        "unitKey": "kg",
        "sourceType": "MANUAL",
    }
    return payload1, payload2


def _post_with_retries(sess: requests.Session, url: str, json_body: dict) -> UploadResult:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = sess.post(url, json=json_body, timeout=30)
            code = r.status_code
            # 가민은 204(본문 없음) / 201(생성) / 200(OK) 등 다양하게 돌려줌
            if code in (200, 201, 204):
                return UploadResult(True, code, r.text, attempt)
            # 409/429/5xx는 잠깐 대기 후 재시도
            if code in (409, 429) or 500 <= code < 600:
                time.sleep(BASE_SLEEP * (2 ** (attempt - 1)))
                continue
            # 그 외는 실패로 본다
            return UploadResult(False, code, r.text, attempt)
        except requests.RequestException as e:
            if attempt == MAX_RETRIES:
                return UploadResult(False, -1, repr(e), attempt)
            time.sleep(BASE_SLEEP * (2 ** (attempt - 1)))
    return UploadResult(False, -1, "unknown", MAX_RETRIES)


def main():
    email = os.environ.get("GARMIN_EMAIL")
    password = os.environ.get("GARMIN_PASSWORD")
    if not email or not password:
        raise SystemExit("환경변수 GARMIN_EMAIL / GARMIN_PASSWORD 가 필요합니다.")

    csv_file, df = _read_latest_csv()
    print(f"[INFO] Selected CSV: {csv_file}")
    print(f"[INFO] Columns: {list(df.columns)}")

    # garth 로그인 (토큰은 기본적으로 ~/.garth 에 캐시됨)
    client = garth.Client()
    client.login(email, password)
    sess = client.connectapi  # requests.Session (가민 쿠키/토큰 세팅 완료)
    print("[INFO] Garmin login OK")

    success = 0
    failed = 0

    for idx, row in df.iterrows():
        try:
            dt_local, kg = _parse_row_to_dt_kg(row)
        except Exception as e:
            print(f"[SKIP] row {idx}: 파싱 실패 - {e}")
            failed += 1
            continue

        # 두 가지 페이로드 순차 시도
        p1, p2 = _payloads_for(dt_local, kg)

        # 시도1
        res = _post_with_retries(sess, WEIGHT_POST_URL, p1)
        if not res.ok:
            # 시도2
            res2 = _post_with_retries(sess, WEIGHT_POST_URL, p2)
            if res2.ok:
                success += 1
                print(f"[OK]  row {idx}: {kg}kg @ {dt_local.isoformat()}  ({res2.status}, attempt={res2.attempt})")
            else:
                failed += 1
                print(f"[FAIL] row {idx}: {kg}kg @ {dt_local.isoformat()}  "
                      f"p1={res.status}/{res.text[:120]!r}, p2={res2.status}/{res2.text[:120]!r}")
        else:
            success += 1
            print(f"[OK]  row {idx}: {kg}kg @ {dt_local.isoformat()}  ({res.status}, attempt={res.attempt})")

    print(f"Done. success={success}, failed={failed}")


if __name__ == "__main__":
    main()
