#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
garmin_weight_uploader.py
- Google Drive에서 내려받은 Google Fit CSV(무게 기록) 중 최신 파일 1개를 읽어
  Garmin Connect에 체중을 업로드합니다.
- garth 0.5.17 대응: 토큰 로딩/저장 API 없이, 위치 인자 login() 사용.
- 업로드는 garth의 인증된 세션으로 Garmin Connect weight-service HTTP 엔드포인트 호출.

필요 환경변수:
  - GARMIN_EMAIL
  - GARMIN_PASSWORD
"""

import csv
import glob
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import garth  # 0.5.17

# ---- 공통 유틸 ----
def log(msg: str) -> None:
    print(msg, flush=True)


def die(msg: str) -> None:
    print(f"[FATAL] {msg}", file=sys.stderr, flush=True)
    sys.exit(1)


# ---- 데이터 모델 ----
@dataclass
class WeightRecord:
    dt: datetime
    weight_kg: float


# ---- CSV 처리 ----
def find_latest_csv(patterns=("*Google Fit.csv", "무게 *.csv")) -> Optional[str]:
    files = []
    for p in patterns:
        files.extend(glob.glob(p))
    if not files:
        return None
    files.sort(key=lambda f: os.path.getmtime(f), reverse=True)
    return files[0]


def parse_weight_from_csv(path: str) -> Optional[WeightRecord]:
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        rows = list(reader)

    if not rows or len(rows) < 2:
        log(f"[WARN] CSV 데이터가 비어있습니다: {path}")
        return None

    header = rows[0]
    data = rows[1]

    def idx(colname_candidates):
        for name in colname_candidates:
            if name in header:
                return header.index(name)
        return -1

    i_date = idx(["날짜", "Date", "date"])
    i_time = idx(["시간", "Time", "time"])
    i_weight = idx(["몸무게", "Body Weight", "Weight", "weight"])

    if i_weight < 0:
        log("[WARN] '몸무게/Weight' 열을 찾을 수 없습니다.")
        return None

    raw_weight = (data[i_weight] or "").replace('"', "").strip()
    if not raw_weight:
        log("[WARN] 몸무게 값이 비어있습니다.")
        return None

    try:
        weight_kg = float(raw_weight)
    except Exception:
        log(f"[WARN] 몸무게 수치 파싱 실패: {raw_weight}")
        return None

    # 날짜/시간 파싱 (없으면 현재 시각 사용)
    if i_date >= 0 and i_time >= 0:
        raw_date = (data[i_date] or "").replace('"', "").strip()
        raw_time = (data[i_time] or "").replace('"', "").strip()
        dt_str = raw_date if len(raw_date) > 10 or not raw_time else f"{raw_date} {raw_time}"
        fmts = [
            "%Y.%m.%d %H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
            "%Y.%m.%d",
            "%Y-%m-%d",
        ]
        parsed = None
        for fmt in fmts:
            try:
                parsed = datetime.strptime(dt_str, fmt)
                break
            except Exception:
                pass
        dt = parsed or datetime.now()
    else:
        dt = datetime.now()

    return WeightRecord(dt=dt, weight_kg=weight_kg)


# ---- Garmin 인증 & 업로드 ----
def ensure_login() -> None:
    """
    garth 0.5.17: 키워드 인자 불가 → 위치 인자 사용.
    토큰 로딩/저장 API 미사용(간단 로그인).
    """
    email = os.environ.get("GARMIN_EMAIL")
    password = os.environ.get("GARMIN_PASSWORD")
    if not email or not password:
        die("GARMIN_EMAIL / GARMIN_PASSWORD 환경변수가 필요합니다.")

    log("[INFO] Garmin SSO 로그인 시도")
    # 키워드가 아닌 '위치 인자'로 전달해야 함
    garth.login(email, password)
    log("[INFO] 로그인 성공")


def upload_weight_with_garth(rec: WeightRecord) -> bool:
    """
    garth의 인증된 세션으로 Garmin Connect weight-service 호출.
    - 신규 레코드: POST /modern/proxy/weight-service/user-weight
    - 타임스탬프 업서트: PUT /modern/proxy/weight-service/user-weight/{epoch_ms}
    어느 한쪽 성공 시 True
    """
    epoch_ms = int(rec.dt.timestamp() * 1000)

    # POST: 새 엔트리 생성
    try:
        payload = {
            "value": rec.weight_kg,
            "unit": "kg",
            "sourceType": "MANUAL",
            "timestamp": epoch_ms,
        }
        # garth에는 고수준 헬퍼가 몇 개 있습니다. JSON 응답을 기대하되, 일부 엔드포인트는 본문이 없을 수 있어
        # 예외 없이 반환되면 성공으로 간주합니다.
        r = garth.connectjson(
            method="POST",
            url="https://connect.garmin.com/modern/proxy/weight-service/user-weight",
            json=payload,
        )
        log("[INFO] POST user-weight 성공")
        return True
    except Exception as e:
        log(f"[WARN] POST user-weight 실패: {e}")

    # PUT: 특정 타임스탬프에 업서트
    try:
        payload = {"value": rec.weight_kg, "unit": "kg", "sourceType": "MANUAL"}
        r = garth.connectjson(
            method="PUT",
            url=f"https://connect.garmin.com/modern/proxy/weight-service/user-weight/{epoch_ms}",
            json=payload,
        )
        log("[INFO] PUT user-weight/{ts} 성공")
        return True
    except Exception as e:
        log(f"[WARN] PUT user-weight 실패: {e}")

    return False


# ---- 실행부 ----
def run() -> None:
    log("[START] weight uploader (garth 0.5.17)")
    ensure_login()

    csv_path = find_latest_csv()
    if not csv_path:
        die("CSV 파일을 찾지 못했습니다.")

    rec = parse_weight_from_csv(csv_path)
    if not rec:
