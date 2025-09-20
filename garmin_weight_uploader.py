#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
garmin_weight_uploader.py

- Google Fit CSV 전체를 읽어 Garmin Connect에 업로드
- Asia/Seoul 타임존 반영
- 중복 제거: (날짜+시간+체중) 기준
"""

import argparse
import glob
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Set, Tuple

import pandas as pd
from dateutil import parser as dtparser
from zoneinfo import ZoneInfo
from garminconnect import Garmin


TOKEN_DIR = os.path.expanduser("~/.garminconnect")

HEADER_MAP = {
    "날짜": "date",
    "시간": "time",
    "몸무게": "weight",
    "체지방률": "percent_fat",
    "총 체수분": "percent_hydration",
    "골량": "bone_mass",
    "근육량": "muscle_mass",
    "기본 대사율": "basal_met",
    "BMI": "bmi",
}


@dataclass
class BodyRow:
    ts_iso: str
    date_str: str
    time_str: str
    weight: float
    percent_fat: Optional[float] = None
    percent_hydration: Optional[float] = None
    bone_mass: Optional[float] = None
    muscle_mass: Optional[float] = None
    basal_met: Optional[float] = None
    bmi: Optional[float] = None

    def dup_key(self) -> Tuple[str, str, float]:
        return (self.date_str, self.time_str, round(self.weight, 2))


def _coerce_float(x) -> Optional[float]:
    try:
        v = float(str(x).strip().replace(",", ".").replace('"', ""))
        return None if v == 0 else v
    except Exception:
        return None


def _parse_timestamp(date_str: str, time_str: Optional[str]) -> datetime:
    s = date_str.strip()
    if time_str:
        if " " not in s and "T" not in s:
            s = f"{s} {time_str.strip()}"
    s = s.replace(".", "-")
    dt = dtparser.parse(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZoneInfo("Asia/Seoul"))
    return dt.replace(microsecond=0)


def _rename_headers(df: pd.DataFrame) -> pd.DataFrame:
    new_cols = {}
    for c in df.columns:
        if c in HEADER_MAP:
            new_cols[c] = HEADER_MAP[c]
        else:
            new_cols[c] = c.lower()
    return df.rename(columns=new_cols)


def load_rows_from_csv(path: str) -> list[BodyRow]:
    df = pd.read_csv(path)
    df = _rename_headers(df)
    if "date" not in df or "weight" not in df:
        return []

    rows: list[BodyRow] = []
    for _, r in df.iterrows():
        date_val = str(r.get("date", "")).strip()
        time_val = str(r.get("time", "")).strip() if "time" in df else ""
        dt_obj = _parse_timestamp(date_val, time_val if time_val else None)

        weight = _coerce_float(r.get("weight"))
        if weight is None:
            continue

        date_s = dt_obj.strftime("%m/%d/%Y")
        time_s = dt_obj.strftime("%I:%M %p").lower().lstrip("0")

        rows.append(
            BodyRow(
                ts_iso=dt_obj.isoformat(),
                date_str=date_s,
                time_str=time_s,
                weight=weight,
                percent_fat=_coerce_float(r.get("percent_fat")),
                percent_hydration=_coerce_float(r.get("percent_hydration")),
                bone_mass=_coerce_float(r.get("bone_mass")),
                muscle_mass=_coerce_float(r.get("muscle_mass")),
                basal_met=_coerce_float(r.get("basal_met")),
                bmi=_coerce_float(r.get("bmi")),
            )
        )
    return rows


def login(email: Optional[str], password: Optional[str]) -> Garmin:
    email = email or os.getenv("GARMIN_EMAIL")
    password = password or os.getenv("GARMIN_PASSWORD")
    if not email or not password:
        sys.exit("GARMIN_EMAIL / GARMIN_PASSWORD 필요")
    api = Garmin(email, password)
    os.makedirs(TOKEN_DIR, exist_ok=True)
    try:
        api.login(token_store=TOKEN_DIR)
    except Exception:
        api.login()
        api.garth.dump(TOKEN_DIR)
    print("✅ Garmin 로그인 성공")
    return api


def upload_rows(api: Garmin, rows: list[BodyRow], dry_run: bool, skip_duplicates: bool) -> None:
    seen: Set[Tuple[str, str, float]] = set()
    for row in rows:
        k = row.dup_key()
        if skip_duplicates and k in seen:
            print(f"⏭️  {row.ts_iso} {row.weight}kg → 중복 스킵")
            continue
        seen.add(k)

        print(f"➡️ {row.ts_iso} {row.weight}kg 업로드 중...")
        if dry_run:
            continue
        try:
            api.add_body_composition(
                row.ts_iso,
                weight=row.weight,
                percent_fat=row.percent_fat,
                percent_hydration=row.percent_hydration,
                bone_mass=row.bone_mass,
                muscle_mass=row.muscle_mass,
                basal_met=row.basal_met,
                bmi=row.bmi,
            )
            print("   ✅ 성공")
        except Exception as e:
            print(f"   ❌ 실패: {e}")
        time.sleep(0.3)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--email")
    ap.add_argument("--password")
    ap.add_argument("--csv", nargs="*", default=["무게*.csv"], help="CSV 패턴 (기본: 무게*.csv 전체)")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--no-skip-duplicates", action="store_true", help="중복도 강제 업로드")
    args = ap.parse_args()

    targets: list[str] = []
    for pat in args.csv:
        targets.extend(glob.glob(pat))
    if not targets:
        sys.exit("CSV 파일을 찾을 수 없습니다.")

    print("📄 처리 대상 CSV:")
    for t in targets:
        print(" -", t)

    api = login(args.email, args.password)

    all_rows: list[BodyRow] = []
    for path in targets:
        rows = load_rows_from_csv(path)
        all_rows.extend(rows)

    print(f"총 {len(all_rows)}개 레코드 로드됨")
    upload_rows(api, all_rows, args.dry_run, skip_duplicates=not args.no_skip_duplicates)


if __name__ == "__main__":
    main()
