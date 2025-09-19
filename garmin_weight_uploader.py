#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Garmin Connect에 체중 기록을 업로드하는 스크립트 (CSV -> Garmin)

- garth 0.5.x 방식(login/resume/save + 전역 garth.client) 사용
- 토큰 캐시 디렉토리: ~/.garth  (GitHub Actions cache로 복원/저장 권장)
- CSV: 구글 드라이브(한글 파일명)에서 내려온 샘플과 동일한 컬럼명 지원
  예시 헤더:
    날짜,시간,몸무게,체지방률,체지방량,무지방 비율,무지방 질량,
    골격근 비율,골격근량,근육량 비율,근육량,골량,총 체수분,기본 대사율
  예시 데이터:
    2025.09.18 12:57:00,12:57:00,"70.2","18.5",...

- 업로드 필드:
    value: kg(실수)
    unitKey: "kg"
    dateTimestamp: epoch milliseconds (ms)
    sourceType: "MANUAL"
- 409(중복)은 성공으로 간주
- 4xx/5xx 시 응답 본문을 디버그 출력

환경변수:
  GARMIN_EMAIL, GARMIN_PASSWORD (필수)
  TZ (선택, 기본 "Asia/Seoul")

사용:
  $ python garmin_weight_uploader.py
  (동일 폴더의 *.csv 중 최신 파일을 자동 선택)
"""

from __future__ import annotations

import os
import sys
import glob
import json
import time
from dataclasses import dataclass
from typing import Optional, List, Tuple

import pandas as pd
from datetime import datetime
from dateutil import tz

import garth
from garth.exc import GarthException
import requests


CSV_GLOB = "*.csv"
DEFAULT_TZ = os.environ.get("TZ", "Asia/Seoul").strip() or "Asia/Seoul"
TOKEN_DIR = os.path.expanduser("~/.garth")

GARMIN_WEIGHT_ENDPOINT = "https://connect.garmin.com/modern/proxy/weight-service/user-weight"


@dataclass
class WeightRecord:
    dt_local: datetime
    weight_kg: float

    @property
    def epoch_ms(self) -> int:
        # Garmin 대부분의 proxy 서비스가 ms를 사용하므로 ms로 변환
        return int(self.dt_local.timestamp() * 1000)


def _log(msg: str) -> None:
    sys.stderr.write(msg.rstrip() + "\n")
    sys.stderr.flush()


def _pick_latest_csv() -> Optional[str]:
    files = glob.glob(CSV_GLOB)
    if not files:
        return None
    files.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return files[0]


def _parse_dt_from_row(row: pd.Series, local_tz: tz.tzfile) -> Optional[datetime]:
    """
    우선순위:
      1) '날짜' 컬럼에 "YYYY.MM.DD HH:MM:SS"가 들어있는 경우 그대로 파싱
      2) '날짜' + '시간' 컬럼이 분리되어 있으면 합쳐서 파싱
      3) 실패 시 None
    """
    date_val = str(row.get("날짜", "")).strip()
    time_val = str(row.get("시간", "")).strip()

    # 케이스 1) '날짜'가 이미 날짜+시간인 경우
    # 예: "2025.09.18 12:57:00"
    for fmt in ("%Y.%m.%d %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(date_val, fmt)
            return dt.replace(tzinfo=local_tz)
        except Exception:
            pass

    # 케이스 2) 날짜와 시간이 분리된 경우
    # 예: 날짜="2025.09.18 00:00:00", 시간="12:57:00" (혹은 날짜="2025.09.18")
    # 날짜 컬럼이 "YYYY.MM.DD HH:MM:SS" 혹은 "YYYY.MM.DD"로 올 수 있으므로 두 경우 모두 처리
    date_only = None
    for fmt in ("%Y.%m.%d %H:%M:%S", "%Y.%m.%d", "%Y-%m-%d"):
        try:
            date_only = datetime.strptime(date_val, fmt)
            break
        except Exception:
            continue

    if date_only is not None:
        # 시간 문자열이 비어 있으면 date_only 그대로 사용
        if time_val and time_val != "00:00:00":
            # HH:MM:SS 혹은 HH:MM 지원
            for tfmt in ("%H:%M:%S", "%H:%M"):
                try:
                    t = datetime.strptime(time_val, tfmt).time()
                    dt = datetime.combine(date_only.date(), t)
                    return dt.replace(tzinfo=local_tz)
                except Exception:
                    continue
        # 시간 정보가 없거나 "00:00:00"인 경우
        return date_only.replace(tzinfo=local_tz)

    return None


def _load_csv(filepath: str, local_tz: tz.tzfile) -> List[WeightRecord]:
    """
    CSV에서 유효한 (dt, kg) 레코드를 추출하여 시간순 정렬해 반환
    """
    try:
        df = pd.read_csv(filepath, encoding="utf-8-sig")
    except Exception:
        # encoding 문제 시 fallback
        df = pd.read_csv(filepath)

    # 필수 컬럼 체크
    if "몸무게" not in df.columns:
        raise ValueError(f"CSV에 '몸무게' 컬럼이 없습니다. 컬럼들: {list(df.columns)}")

    recs: List[WeightRecord] = []
    for _, row in df.iterrows():
        # 몸무게 파싱
        w = row.get("몸무게", None)
        if w is None:
            continue
        try:
            weight_kg = float(str(w).replace(",", "").strip().strip('"'))
        except Exception:
            continue
        if not (0.0 < weight_kg < 400.0):  # 비현실값 필터
            continue

        # datetime 파싱
        dt = _parse_dt_from_row(row, local_tz)
        if dt is None:
            continue

        recs.append(WeightRecord(dt_local=dt, weight_kg=weight_kg))

    # 시간순 정렬(오래된 것 -> 최신)
    recs.sort(key=lambda r: r.dt_local)
    return recs


def _login_garmin(email: str, password: str, token_dir: str = TOKEN_DIR):
    """
    garth 0.5.x 방식의 세션 복구/로그인
    - 먼저 resume(token_dir)
    - 유효성 확인 실패 시 login → save(token_dir)
    - 성공 시 전역 garth.client 사용 가능
    """
    try:
        garth.resume(token_dir)
        # 세션 유효 확인 (username 접근 가능해야 함)
        _ = garth.client.username  # noqa: F841
    except Exception:
        garth.login(email, password)
        garth.save(token_dir)
    return garth.client


def _post_weight(session: requests.Session, record: WeightRecord) -> Tuple[int, str]:
    """
    Garmin weight-service 엔드포인트로 단일 레코드 업로드
    - 201/200/409(중복) → 성공 취급
    - 그 외 → (status, body) 반환
    """
    payload = {
        "value": record.weight_kg,
        "unitKey": "kg",
        "dateTimestamp": record.epoch_ms,  # epoch milliseconds
        "sourceType": "MANUAL",
    }

    # session은 garth.client.session 사용
    resp = session.post(GARMIN_WEIGHT_ENDPOINT, json=payload)
    text = ""
    try:
        text = resp.text
    except Exception:
        text = "<no text>"

    return resp.status_code, text


def main() -> None:
    local_tz = tz.gettz(DEFAULT_TZ)
    if local_tz is None:
        _log(f"[WARN] TZ='{DEFAULT_TZ}' 인식 실패. 'Asia/Seoul' 사용")
        local_tz = tz.gettz("Asia/Seoul")

    email = os.environ.get("GARMIN_EMAIL", "").strip()
    password = os.environ.get("GARMIN_PASSWORD", "").strip()
    if not email or not password:
        _log("[ERROR] GARMIN_EMAIL / GARMIN_PASSWORD 환경변수를 설정하세요.")
        sys.exit(2)

    # 최신 CSV 자동 선택
    csv_path = _pick_latest_csv()
    if not csv_path:
        _log("[ERROR] 작업 폴더에서 *.csv 파일을 찾지 못했습니다.")
        sys.exit(3)

    _log(f"[INFO] Selected CSV: {csv_path}")

    # CSV 로드
    records = _load_csv(csv_path, local_tz)
    if not records:
        _log("[ERROR] CSV에서 유효한 체중 레코드를 찾지 못했습니다.")
        sys.exit(4)

    # 로그인/세션
    client = _login_garmin(email, password, TOKEN_DIR)
    session = client.session  # requests.Session (garth가 쿠키/토큰 유지)

    # 업로드: CSV의 모든 레코드를 순회(중복은 409로 무시)
    ok, dup, fail = 0, 0, 0
    last_status = None

    for rec in records:
        status, body = _post_weight(session, rec)
        last_status = status

        if status in (200, 201):
            ok += 1
            _log(f"[OK] {rec.dt_local.isoformat()}  {rec.weight_kg:.3f} kg  → {status}")
        elif status == 409:
            dup += 1
            _log(f"[DUP] {rec.dt_local.isoformat()}  {rec.weight_kg:.3f} kg  → 409 (already exists)")
        else:
            fail += 1
            _log(f"[ERR] {rec.dt_local.isoformat()}  {rec.weight_kg:.3f} kg  → {status}")
            # 본문이 JSON이면 이쁘게
            try:
                j = json.loads(body)
                pretty = json.dumps(j, ensure_ascii=False, indent=2)
                _log(pretty)
            except Exception:
                _log(body[:2000])

            # 비정상 응답이라도 계속 진행(한 레코드 실패가 전체를 막지 않도록)

    _log(f"[SUMMARY] success={ok}, duplicate={dup}, failed={fail}")

    # 전체가 실패면 비정상 종료
    if ok == 0 and dup == 0:
        _log("[ERROR] 업로드에 모두 실패했습니다.")
        sys.exit(1)

    # 일부라도 성공/중복이면 0 종료
    sys.exit(0)


if __name__ == "__main__":
    main()
