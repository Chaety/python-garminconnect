#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
garmin_weight_uploader.py

- Google Drive로 동기화된 CSV/FIT 파일 전체를 읽어
  Garmin Connect에 '체중'만 업로드합니다.

요구사항
  * CSV + FIT 모든 레코드 처리
  * CSV 시간은 12시간제(AM/PM) 우선, 24시간제도 백업 파싱
  * 업로드 시 날짜는 MM/DD/YYYY, 시간은 hh:mm am/pm (소문자, 초 없음)
  * 중복 스킵 기준: (MM/DD/YYYY, hh:mm am/pm, 체중)
  * 가급적 문자열(date, time) 필드를 사용해 업로드 시도.
    실패 시 타임스탬프(ms) 방식으로 자동 폴백.

환경변수
  - GARMIN_EMAIL
  - GARMIN_PASSWORD
"""

import os
import sys
import re
import glob
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Set, Tuple, Optional

import pandas as pd
import garth

# FIT 지원은 선택 사항: 미설치면 .fit 파일은 건너뜀
try:
    from fitparse import FitFile  # type: ignore
    FIT_ENABLED = True
except Exception:
    FIT_ENABLED = False


# ------------------------------------------------------
# 데이터 구조
# ------------------------------------------------------
@dataclass
class WeightRecord:
    dt: datetime            # 로컬 기준 datetime (초 제거)
    weight_kg: float

    def normalized(self) -> datetime:
        return self.dt.replace(second=0, microsecond=0)

    def date_str_mmddyyyy(self) -> str:
        """MM/DD/YYYY"""
        return self.normalized().strftime("%m/%d/%Y")

    def time_str_12h_lower(self) -> str:
        """hh:mm am/pm  (소문자, 앞자리 0 제거)"""
        t = self.normalized().strftime("%I:%M %p").lower()
        return re.sub(r"^0", "", t)  # 07:05 pm -> 7:05 pm

    def dup_key(self) -> Tuple[str, str, float]:
        """중복 판단 키: (날짜, 시간, 체중[소수2자리])"""
        return (self.date_str_mmddyyyy(), self.time_str_12h_lower(), round(self.weight_kg, 2))


# ------------------------------------------------------
# 유틸: 문자열 -> datetime 파싱 (CSV용)
# ------------------------------------------------------
_AMPM_TIME_FORMATS = [
    "%I:%M %p",       # 7:05 PM
    "%I:%M:%S %p",    # 07:05:00 AM
]
_24H_TIME_FORMATS = [
    "%H:%M",
    "%H:%M:%S",
]
_DATE_FORMATS = [
    "%Y.%m.%d",       # 2025.09.19
    "%Y-%m-%d",
    "%Y/%m/%d",
    "%m/%d/%Y",       # 이미 미국식으로 올 수도 있음
]


def _parse_csv_datetime(date_str: str, time_str: str) -> Optional[datetime]:
    """
    CSV의 '날짜', '시간'을 합쳐 datetime으로 파싱.
    - 우선 12시간제 AM/PM
    - 실패 시 24시간제
    반환값은 naive datetime (초 제거).
    """
    date_str = str(date_str).strip()
    # 한글 오전/오후도 AM/PM으로 치환
    ts = str(time_str).strip().upper().replace("오전", "AM").replace("오후", "PM")

    for dfmt in _DATE_FORMATS:
        # 12시간제
        for tfmt in _AMPM_TIME_FORMATS:
            try:
                dt = datetime.strptime(f"{date_str} {ts}", f"{dfmt} {tfmt}")
                return dt.replace(second=0, microsecond=0)
            except Exception:
                pass
        # 24시간제
        for tfmt in _24H_TIME_FORMATS:
            try:
                dt = datetime.strptime(f"{date_str} {ts}", f"{dfmt} {tfmt}")
                return dt.replace(second=0, microsecond=0)
            except Exception:
                pass
    return None


def _to_float_weight(val) -> Optional[float]:
    try:
        s = str(val).strip().replace(",", "").replace('"', "")
        if s == "" or s.lower() == "nan":
            return None
        return float(s)
    except Exception:
        return None


# ------------------------------------------------------
# CSV / FIT 파서
# ------------------------------------------------------
def parse_csv(path: str) -> List[WeightRecord]:
    """
    기대 컬럼(한글 기준): 날짜, 시간, 몸무게
    - 영문 변형(date,time,weight)도 자동 탐색
    - 'datetime' 단일 칼럼만 있을 경우도 처리
    """
    out: List[WeightRecord] = []
    try:
        df = pd.read_csv(path)
    except Exception as e:
        print(f"[WARN] CSV 읽기 실패: {path} ({e})")
        return out

    cols = {c.strip(): c for c in df.columns}
    cand_date = next((cols[c] for c in cols if c in ("날짜", "date", "Date", "DATE")), None)
    cand_time = next((cols[c] for c in cols if c in ("시간", "time", "Time", "TIME")), None)
    cand_weight = next((cols[c] for c in cols if c in ("몸무게", "weight", "Weight", "WEIGHT")), None)

    # 날짜/시간이 합쳐진 단일 칼럼 케이스
    cand_dt = next((cols[c] for c in cols if c.lower() in ("datetime", "일시", "측정시각")), None)

    if cand_dt and cand_weight and not (cand_date and cand_time):
        for _, row in df.iterrows():
            w = _to_float_weight(row.get(cand_weight))
            if w is None:
                continue
            raw = row.get(cand_dt)
            dt_obj: Optional[datetime] = None
            if isinstance(raw, str):
                for fmt in (
                    "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %I:%M %p",
                    "%Y/%m/%d %H:%M", "%Y.%m.%d %H:%M:%S",
                    "%Y.%m.%d %I:%M %p", "%m/%d/%Y %I:%M %p",
                ):
                    try:
                        dt_obj = datetime.strptime(raw.strip(), fmt)
                        break
                    except Exception:
                        pass
            elif isinstance(raw, pd.Timestamp):
                dt_obj = raw.to_pydatetime()
            if not dt_obj:
                continue
            dt_obj = dt_obj.replace(second=0, microsecond=0)
            out.append(WeightRecord(dt=dt_obj, weight_kg=w))
        return out

    if not (cand_date and cand_time and cand_weight):
        print(f"[WARN] 필요한 컬럼을 찾을 수 없음: {path}")
        return out

    for _, row in df.iterrows():
        w = _to_float_weight(row.get(cand_weight))
        if w is None:
            continue
        dt_obj = _parse_csv_datetime(row.get(cand_date), row.get(cand_time))
        if not dt_obj:
            continue
        out.append(WeightRecord(dt=dt_obj, weight_kg=w))
    return out


def parse_fit(path: str) -> List[WeightRecord]:
    out: List[WeightRecord] = []
    if not FIT_ENABLED:
        print(f"[INFO] fitparse 미설치, FIT 스킵: {os.path.basename(path)}")
        return out
    try:
        fitfile = FitFile(path)
        for msg in fitfile.get_messages("weight_scale"):
            ts, weight = None, None
            for f in msg:
                if f.name == "timestamp":
                    ts = f.value
                elif f.name == "weight":
                    weight = f.value
            if ts and weight:
                dt_obj = ts.replace(second=0, microsecond=0)
                out.append(WeightRecord(dt=dt_obj, weight_kg=float(weight)))
    except Exception as e:
        print(f"[WARN] FIT 파싱 실패: {path} ({e})")
    return out


def load_all_records(folder: str = ".") -> List[WeightRecord]:
    all_recs: List[WeightRecord] = []
    for p in sorted(glob.glob(os.path.join(folder, "*.csv"))):
        all_recs.extend(parse_csv(p))
    for p in sorted(glob.glob(os.path.join(folder, "*.fit"))):
        all_recs.extend(parse_fit(p))
    return all_recs


# ------------------------------------------------------
# Garmin 업로드
# ------------------------------------------------------
def login_with_garth():
    email = os.environ.get("GARMIN_EMAIL")
    password = os.environ.get("GARMIN_PASSWORD")
    if not email or not password:
        sys.exit("[FATAL] GARMIN_EMAIL / GARMIN_PASSWORD 필요")

    print("[INFO] Garmin SSO 로그인 시도")
    garth.login(email, password)
    if not getattr(garth, "client", None):
        sys.exit("[FATAL] garth 로그인 실패")
    print("[INFO] 로그인 성공")


def _post_weight_with_date_time(weight_kg: float, date_str: str, time_str: str) -> bool:
    """
    1차 시도: 문자열 date/time로 업로드
    - date: MM/DD/YYYY
    - time: hh:mm am/pm
    """
    payload = {
        "value": float(weight_kg),   # 체중 숫자만
        "unit": "kg",
        "sourceType": "MANUAL",
        "date": date_str,
        "time": time_str,
    }
    resp = garth.client.post(
        "connectapi",
        "proxy/weight-service/user-weight",
        json=payload
    )
    return getattr(resp, "status_code", None) in (200, 201, 204)


def _post_weight_with_timestamp(rec: WeightRecord) -> bool:
    """
    폴백: 타임스탬프(ms) 업로드
    """
    ndt = rec.normalized()
    if ndt.tzinfo is None:
        dt_for_epoch = ndt.replace(tzinfo=timezone.utc)
    else:
        dt_for_epoch = ndt.astimezone(timezone.utc)
    epoch_ms = int(dt_for_epoch.timestamp() * 1000)

    payload = {
        "value": float(rec.weight_kg),
        "unit": "kg",
        "sourceType": "MANUAL",
        "timestamp": epoch_ms,
    }
    resp = garth.client.post(
        "connectapi",
        "proxy/weight-service/user-weight",
        json=payload
    )
    return getattr(resp, "status_code", None) in (200, 201, 204)


def upload_weight(rec: WeightRecord) -> bool:
    """
    우선 (date, time) 문자열 방식으로 시도하고,
    실패 시 timestamp 방식으로 폴백.
    """
    date_s = rec.date_str_mmddyyyy()
    time_s = rec.time_str_12h_lower()
    try:
        if _post_weight_with_date_time(rec.weight_kg, date_s, time_s):
            print(f"[OK] 업로드 성공: {date_s} {time_s}  {rec.weight_kg:.2f} kg")
            return True
        # 폴백
        if _post_weight_with_timestamp(rec):
            print(f"[OK] 업로드 성공(폴백): {date_s} {time_s}  {rec.weight_kg:.2f} kg")
            return True
        print(f"[SKIP] 업로드 실패: {date_s} {time_s}  {rec.weight_kg:.2f} kg")
        return False
    except TypeError as te:
        print(f"[ERR] 클라이언트 호출 오류(TypeError): {te}")
        return False
    except Exception as e:
        print(f"[ERR] 예외: {e}")
        return False


# ------------------------------------------------------
# 실행
# ------------------------------------------------------
def run():
    print(f"[START] weight uploader (garth {getattr(garth, '__version__', 'unknown')})")
    records = load_all_records(".")
    if not records:
        print("[INFO] 레코드 없음")
        return

    # 날짜/시간/체중 기반 중복 제거
    #   날짜(MM/DD/YYYY), 시간(hh:mm am/pm), 체중(소수2자리)
    dedup: Set[Tuple[str, str, float]] = set()
    unique: List[WeightRecord] = []
    for r in sorted(records, key=lambda x: x.normalized()):
        k = r.dup_key()
        if k in dedup:
            print(f"[SKIP] 중복 스킵: {k}")
            continue
        dedup.add(k)
        unique.append(r)

    login_with_garth()

    uploaded, skipped = 0, 0
    for rec in unique:
        if upload_weight(rec):
            uploaded += 1
        else:
            skipped += 1

    print(f"[DONE] 업로드 {uploaded}건, 스킵 {skipped}건")


if __name__ == "__main__":
    run()
