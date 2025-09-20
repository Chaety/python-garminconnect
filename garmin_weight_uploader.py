#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
garmin_weight_uploader.py

- 구글드라이브 동기화된 CSV/FIT 파일 전체에서 체중만 읽어
  Garmin Connect에 업로드
- 날짜: MM/DD/YYYY
- 시간: h:mm am/pm (소문자, 초 없음)
- 중복: (날짜, 시간, 체중) 기준으로 스킵
- 엔드포인트: proxy/wellness-service/user-weight
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

try:
    from fitparse import FitFile
    FIT_ENABLED = True
except Exception:
    FIT_ENABLED = False


@dataclass
class WeightRecord:
    dt: datetime
    weight_kg: float

    def normalized(self) -> datetime:
        return self.dt.replace(second=0, microsecond=0)

    def date_str(self) -> str:
        return self.normalized().strftime("%m/%d/%Y")

    def time_str(self) -> str:
        t = self.normalized().strftime("%I:%M %p").lower()
        return re.sub(r"^0", "", t)

    def dup_key(self) -> Tuple[str, str, float]:
        return (self.date_str(), self.time_str(), round(self.weight_kg, 2))


def _to_float_weight(val) -> Optional[float]:
    try:
        return float(str(val).strip().replace('"', '').replace(',', ''))
    except Exception:
        return None


def parse_csv(path: str) -> List[WeightRecord]:
    out: List[WeightRecord] = []
    try:
        df = pd.read_csv(path)
    except Exception as e:
        print(f"[WARN] CSV 읽기 실패: {path} ({e})")
        return out

    cols = {c.strip(): c for c in df.columns}
    col_date = next((cols[c] for c in cols if c in ("날짜", "date", "Date")), None)
    col_time = next((cols[c] for c in cols if c in ("시간", "time", "Time")), None)
    col_weight = next((cols[c] for c in cols if c in ("몸무게", "weight", "Weight")), None)

    if not col_weight:
        print(f"[WARN] 몸무게 컬럼 없음: {path}")
        return out

    for _, row in df.iterrows():
        w = _to_float_weight(row.get(col_weight))
        if w is None:
            continue

        dval, tval = str(row.get(col_date, "")).strip(), str(row.get(col_time, "")).strip()
        dt_obj: Optional[datetime] = None

        # case1: 날짜 안에 시각까지 같이 있음
        if re.match(r"^\d{4}[./-]\d{2}[./-]\d{2} \d{2}:\d{2}:\d{2}$", dval):
            for fmt in ("%Y.%m.%d %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
                try:
                    dt_obj = datetime.strptime(dval, fmt)
                    break
                except Exception:
                    pass

        # case2: 날짜 + 시간 따로
        if not dt_obj and dval and tval:
            for fmt in ("%Y.%m.%d %H:%M:%S", "%Y.%m.%d %H:%M"):
                try:
                    dt_obj = datetime.strptime(f"{dval} {tval}", fmt)
                    break
                except Exception:
                    pass

        if dt_obj:
            dt_obj = dt_obj.replace(second=0, microsecond=0)
            out.append(WeightRecord(dt=dt_obj, weight_kg=w))

    return out


def parse_fit(path: str) -> List[WeightRecord]:
    out: List[WeightRecord] = []
    if not FIT_ENABLED:
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


def load_all_records(folder=".") -> List[WeightRecord]:
    recs: List[WeightRecord] = []
    for p in sorted(glob.glob(os.path.join(folder, "*.csv"))):
        recs.extend(parse_csv(p))
    for p in sorted(glob.glob(os.path.join(folder, "*.fit"))):
        recs.extend(parse_fit(p))
    return recs


def login():
    email, pw = os.environ.get("GARMIN_EMAIL"), os.environ.get("GARMIN_PASSWORD")
    if not email or not pw:
        sys.exit("[FATAL] GARMIN_EMAIL / GARMIN_PASSWORD 필요")
    garth.login(email, pw)
    print("[INFO] 로그인 성공")


def upload_weight(rec: WeightRecord) -> bool:
    payload = {
        "value": rec.weight_kg,
        "unit": "kg",
        "sourceType": "MANUAL",
        "date": rec.date_str(),
        "time": rec.time_str(),
    }
    try:
        r = garth.client.post("connectapi", "proxy/wellness-service/user-weight", json=payload)
        if r.status_code in (200, 201, 204):
            print(f"[OK] {rec.date_str()} {rec.time_str()} {rec.weight_kg}kg 업로드")
            return True
        else:
            print(f"[FAIL] {rec.date_str()} {rec.time_str()} status={r.status_code} body={r.text[:200]}")
    except Exception as e:
        print(f"[ERR] 업로드 실패: {e}")
    return False


def run():
    print("[START] weight uploader")
    recs = load_all_records(".")
    if not recs:
        print("[INFO] 레코드 없음")
        return

    seen: Set[Tuple[str, str, float]] = set()
    uniq: List[WeightRecord] = []
    for r in sorted(recs, key=lambda x: x.normalized()):
        if r.dup_key() in seen:
            print(f"[SKIP] 중복: {r.dup_key()}")
            continue
        seen.add(r.dup_key())
        uniq.append(r)

    login()
    ok, fail = 0, 0
    for r in uniq:
        if upload_weight(r):
            ok += 1
        else:
            fail += 1
    print(f"[DONE] 업로드 {ok}, 실패 {fail}")


if __name__ == "__main__":
    run()
