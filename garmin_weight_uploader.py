#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
garmin_weight_uploader.py

- Google Drive로부터 동기된 CSV(예: "무게 YYYY.MM.DD Google Fit.csv")에서 가장 최신 레코드를 읽어
  Garmin Connect 계정에 체중을 업로드합니다.
- garth 0.5.x 기준 API에 맞춰 수정됨:
  * garth.login(email, password)  ← 위치 인자만 사용
  * garth.client                  ← 로그인 후 사용할 HTTP 세션
  * 더 이상 존재하지 않는 garth.load_token / garth.connectjson 사용 안 함

필요 환경변수:
- GARMIN_EMAIL
- GARMIN_PASSWORD
"""

from __future__ import annotations

import os
import sys
import glob
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Tuple

import pandas as pd

try:
    import garth  # garth==0.5.17 기준
except Exception as e:  # pragma: no cover
    print(f"[FATAL] garth 임포트 실패: {e}")
    sys.exit(1)


# -------------------------------
# 유틸
# -------------------------------
def log(msg: str) -> None:
    print(msg, flush=True)


def fail(msg: str, code: int = 1) -> None:  # pragma: no cover
    log(msg)
    sys.exit(code)


# -------------------------------
# 데이터 모델
# -------------------------------
@dataclass
class WeightRecord:
    dt: datetime       # 측정 시각 (naive, local 기준)
    weight_kg: float   # 체중 (kg)


# -------------------------------
# CSV 파싱
# -------------------------------
KOR_COL_DATE = "날짜"
KOR_COL_TIME = "시간"
KOR_COL_WEIGHT = "몸무게"


def _parse_datetime_kr(date_str: str, time_str: str) -> datetime:
    """
    date_str 예: '2025.09.19 10:43:00' 또는 '2025.09.19 10:43:00'가 날짜 컬럼에만/혹은 날짜+시간 모두 담길 수 있음.
    time_str  예: '10:43:00'
    """
    date_str = str(date_str).strip()
    time_str = str(time_str).strip()

    # 'YYYY.MM.DD HH:MM:SS'가 날짜 컬럼에 같이 오는 경우 처리
    if " " in date_str and time_str:
        # 날짜 컬럼에 시간까지 이미 있음 → date_str 우선
        try:
            return datetime.strptime(date_str, "%Y.%m.%d %H:%M:%S")
        except ValueError:
            pass

    # 일반 케이스: 날짜와 시간을 합침
    comb = f"{date_str} {time_str}".strip()
    for fmt in ("%Y.%m.%d %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(comb, fmt)
        except ValueError:
            continue

    # 날짜만 있는 경우 마지막 시도로 처리
    for fmt in ("%Y.%m.%d", "%Y-%m-%d"):
        try:
            d = datetime.strptime(date_str, fmt)
            return d
        except ValueError:
            continue

    raise ValueError(f"지원되지 않는 날짜/시간 형식: date='{date_str}', time='{time_str}'")


def _coerce_float(value) -> float:
    try:
        # 문자열에 따옴표, 콤마 등 제거
        s = str(value).replace(",", "").replace('"', "").strip()
        return float(s)
    except Exception as e:
        raise ValueError(f"숫자 변환 실패: {value!r} ({e})")


def read_latest_weight_from_csv(folder: str = ".") -> WeightRecord:
    """
    작업 디렉터리에서 가장 최신 CSV를 찾아 한 줄(가장 최신 측정)만 읽어 WeightRecord로 반환.
    파일명 패턴은 제한하지 않고 *.csv 중 수정시각이 최신인 파일을 사용.
    """
    candidates = glob.glob(os.path.join(folder, "*.csv"))
    if not candidates:
        raise FileNotFoundError("CSV 파일을 찾을 수 없습니다 (*.csv). rclone copy가 올바르게 수행되었는지 확인하세요.")

    latest_path = max(candidates, key=os.path.getmtime)
    log(f"[INFO] 최신 CSV: {os.path.basename(latest_path)}")

    # 헤더가 한글인 CSV (구글핏 내보내기 예시)
    df = pd.read_csv(latest_path)

    for col in (KOR_COL_DATE, KOR_COL_TIME, KOR_COL_WEIGHT):
        if col not in df.columns:
            raise KeyError(f"CSV 컬럼 누락: '{col}' (존재 컬럼: {list(df.columns)})")

    # 가장 첫 행을 사용 (파일이 단일 레코드인 경우가 많음)
    row = df.iloc[0]
    dt = _parse_datetime_kr(row[KOR_COL_DATE], row[KOR_COL_TIME])
    weight = _coerce_float(row[KOR_COL_WEIGHT])

    return WeightRecord(dt=dt, weight_kg=weight)


# -------------------------------
# 로그인
# -------------------------------
def login_with_garth(email: str, password: str) -> None:
    """
    garth 0.5.x:
      - garth.login(email, password)  ← 위치 인자만 사용(키워드 인자 사용 시 TypeError 발생)
      - 이후 garth.client에 세션이 세팅됨
    """
    if not email or not password:
        raise ValueError("환경변수 GARMIN_EMAIL/GARMIN_PASSWORD가 설정되어야 합니다.")

    log("[INFO] Garmin SSO 로그인 시도")
    # 위치 인자로만 호출 (keyword 사용하면 TypeError)
    garth.login(email, password)
    if not getattr(garth, "client", None):
        raise RuntimeError("로그인 후 garth.client가 초기화되지 않았습니다.")
    log("[INFO] 로그인 성공")


# -------------------------------
# 업로드
# -------------------------------
def upload_weight_with_garth(rec: WeightRecord) -> bool:
    """
    garth의 인증 세션(garth.client)로 Garmin Connect weight-service 호출.
    1) POST 새 레코드
    2) 실패 시 동일 타임스탬프 업서트 PUT
    """
    if not hasattr(garth, "client") or garth.client is None:
        log("[WARN] garth.client가 초기화되지 않았습니다. 로그인 상태를 확인하세요.")
        return False

    c = garth.client  # garth.http.Client 인스턴스
    epoch_ms = int(rec.dt.timestamp() * 1000)

    headers = {
        "accept": "application/json, text/plain, */*",
        "content-type": "application/json;charset=UTF-8",
        "origin": "https://connect.garmin.com",
        "referer": "https://connect.garmin.com/",
    }

    # 1) POST: 새 엔트리 생성
    try:
        payload = {
            "value": rec.weight_kg,
            "unit": "kg",
            "sourceType": "MANUAL",
            "timestamp": epoch_ms,
        }
        r = c.post(
            "https://connect.garmin.com/modern/proxy/weight-service/user-weight",
            json=payload,
            headers=headers,
        )
        if r.status_code in (200, 201, 204):
            log("[INFO] POST user-weight 성공")
            return True
        else:
            log(f"[WARN] POST 실패 status={r.status_code} body={r.text[:200]}")
    except Exception as e:
        log(f"[WARN] POST 예외: {e}")

    # 2) PUT: 타임스탬프 업서트
    try:
        payload = {
            "value": rec.weight_kg,
            "unit": "kg",
            "sourceType": "MANUAL",
        }
        r = c.put(
            f"https://connect.garmin.com/modern/proxy/weight-service/user-weight/{epoch_ms}",
            json=payload,
            headers=headers,
        )
        if r.status_code in (200, 201, 204):
            log("[INFO] PUT user-weight/{ts} 성공")
            return True
        else:
            log(f"[WARN] PUT 실패 status={r.status_code} body={r.text[:200]}")
    except Exception as e:
        log(f"[WARN] PUT 예외: {e}")

    return False


# -------------------------------
# 실행
# -------------------------------
def run() -> None:
    log(f"[START] weight uploader (garth {getattr(garth, '__version__', 'unknown')})")

    # 1) 최신 CSV에서 레코드 추출
    rec = read_latest_weight_from_csv(".")
    log(f"[INFO] 업로드 대상: {rec.weight_kg} kg @ {rec.dt:%Y-%m-%d %H:%M:%S}")

    # 2) 로그인
    email = os.environ.get("GARMIN_EMAIL", "")
    password = os.environ.get("GARMIN_PASSWORD", "")
    login_with_garth(email, password)

    # 3) 업로드
    ok = upload_weight_with_garth(rec)
    if not ok:
        fail("[FATAL] 업로드 실패 (엔드포인트/계정 정책 변경 가능성)")

    log("[DONE] 업로드 완료")


if __name__ == "__main__":
    try:
        run()
    except Exception as e:  # pragma: no cover
        # 스택 트레이스는 CI 로그를 지저분하게 만들 수 있어 메시지만 출력
        log(f"[FATAL] 예외: {e}")
        sys.exit(1)
