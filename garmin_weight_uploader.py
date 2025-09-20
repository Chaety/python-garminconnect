#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
garmin_weight_uploader.py
- Google Fit에서 내보낸 체중 CSV를 읽어 Garmin Connect에 '체성분(Body Composition)'으로 업로드
- python-garminconnect v0.2.30 기준 라이브러리 메서드(api.add_body_composition) 사용

예)
  python garmin_weight_uploader.py \
      --email "$GARMIN_EMAIL" --password "$GARMIN_PASSWORD" \
      --csv "무게 *.csv" --dry-run

GitHub Actions에서 환경변수 사용:
  GARMIN_EMAIL / GARMIN_PASSWORD 를 환경변수로 주고 --email/--password 생략 가능
"""

from __future__ import annotations

import argparse
import glob
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Iterable

import pandas as pd
from dateutil import parser as dtparser
from garminconnect import Garmin  # python-garminconnect==0.2.30


# -------- 설정 -------- #
TOKEN_DIR = os.path.expanduser("~/.garminconnect")  # 토큰 캐시 경로
KOREAN_HEADER_MAP = {
    "날짜": "date",
    "시간": "time",
    "몸무게": "weight",
    "체지방률": "percent_fat",
    "총 체수분": "percent_hydration",
    "골량": "bone_mass",
    "근육량": "muscle_mass",
    "기본 대사율": "basal_met",
    # 아래는 구글핏/체중 CSV에서 종종 보이는 칼럼들(없어도 됨)
    "체지방량": "fat_mass",
    "무지방 비율": "lean_percent",
    "무지방 질량": "lean_mass",
    "골격근 비율": "skeletal_muscle_percent",
    "골격근량": "skeletal_muscle_mass",
    "근육량 비율": "muscle_percent",
    "BMI": "bmi",
}
ACCEPTED_EN_HEADERS = {
    "date",
    "time",
    "weight",
    "percent_fat",
    "percent_hydration",
    "bone_mass",
    "muscle_mass",
    "basal_met",
    "fat_mass",
    "lean_percent",
    "lean_mass",
    "skeletal_muscle_percent",
    "skeletal_muscle_mass",
    "muscle_percent",
    "bmi",
}


@dataclass
class BodyRow:
    ts_iso: str
    weight: float
    # 옵션 필드들
    percent_fat: Optional[float] = None
    percent_hydration: Optional[float] = None
    bone_mass: Optional[float] = None
    muscle_mass: Optional[float] = None
    basal_met: Optional[float] = None
    bmi: Optional[float] = None


def _coerce_float(x) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip().replace(",", ".").replace('"', "")
    if s == "" or s == "0" or s.lower() == "none" or s.lower() == "null":
        return None
    try:
        v = float(s)
        # 0.0은 의미없는 값인 경우가 많아 None 취급
        return None if abs(v) < 1e-12 else v
    except Exception:
        return None


def _parse_timestamp(date_str: str, time_str: Optional[str]) -> str:
    """
    - date_str가 이미 'YYYY.MM.DD HH:MM:SS' 같은 전체 타임스탬프면 그대로 파싱
    - 아니면 date + time 결합
    - 가민 라이브러리는 ISO8601 문자열을 받아줌 (예: '2025-09-19T10:43:00')
    """
    s = (date_str or "").strip()
    if time_str and time_str.strip():
        # date가 날짜만 들어있다면 합치기
        if " " not in s and "T" not in s:
            s = f"{s} {time_str.strip()}"
    # 구글핏 CSV는 '2025.09.19 10:43:00' 같이 구분자가 '.' 인 경우가 많음
    s = s.replace(".", "-")
    dt = dtparser.parse(s)
    # 타임존이 없으면 naive → 로컬로 간주, 가민은 TZ 없는 ISO도 허용
    return dt.replace(microsecond=0).isoformat()


def _rename_headers(df: pd.DataFrame) -> pd.DataFrame:
    new_cols = {}
    for c in df.columns:
        c_strip = str(c).strip()
        if c_strip in KOREAN_HEADER_MAP:
            new_cols[c] = KOREAN_HEADER_MAP[c_strip]
        else:
            # 영문 헤더면 소문자 통일
            new_cols[c] = c_strip.lower()
    df = df.rename(columns=new_cols)
    # 불필요한/알수없는 헤더는 그대로 두되, 우리가 쓰는 컬럼만 접근
    return df


def load_rows_from_csv(path: str) -> list[BodyRow]:
    df = pd.read_csv(path)
    df = _rename_headers(df)

    # 최소 요구 컬럼: date, weight (time은 없어도 됨)
    if "date" not in df.columns or "weight" not in df.columns:
        raise ValueError(f"필수 칼럼(date/weight) 누락 - 파일: {path}")

    rows: list[BodyRow] = []
    for _, r in df.iterrows():
        date_val = str(r.get("date", "")).strip()
        time_val = str(r.get("time", "")).strip() if "time" in df.columns else ""
        # 어떤 파일은 date 칼럼에 이미 'YYYY.MM.DD HH:MM:SS'가 들어있음
        ts_iso = _parse_timestamp(date_val, time_val if time_val else None)

        weight = _coerce_float(r.get("weight"))
        if weight is None:
            continue  # 몸무게 없으면 스킵

        rows.append(
            BodyRow(
                ts_iso=ts_iso,
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


def choose_latest_csv(patterns: Iterable[str]) -> Optional[str]:
    cands: list[str] = []
    for p in patterns:
        cands.extend(glob.glob(p))
    cands = [c for c in cands if c.lower().endswith(".csv")]
    if not cands:
        return None
    cands.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return cands[0]


def login(email: Optional[str], password: Optional[str]) -> Garmin:
    email = email or os.getenv("GARMIN_EMAIL")
    password = password or os.getenv("GARMIN_PASSWORD")
    if not email or not password:
        print("❌ 이메일/비밀번호가 필요합니다. (--email/--password 또는 환경변수 GARMIN_EMAIL/GARMIN_PASSWORD)")
        sys.exit(1)

    api = Garmin(email, password)
    # 토큰 캐시 사용
    os.makedirs(TOKEN_DIR, exist_ok=True)
    try:
        api.login(token_store=TOKEN_DIR)
    except Exception:
        # 토큰 깨졌을 때 재로그인
        api.login()
        api.garth.dump(TOKEN_DIR)
    print("✅ Garmin 로그인 성공")
    return api


def fetch_today_weights_map(api: Garmin, iso_date: str) -> set[float]:
    """
    같은 날짜의 중복 업로드 방지를 위해, 이미 등록된 체중값(kg)을 셋으로 반환.
    """
    try:
        data = api.get_daily_weigh_ins(iso_date)
        exist: set[float] = set()
        if data and isinstance(data, dict) and "dateWeightList" in data:
            for item in data["dateWeightList"]:
                if not isinstance(item, dict):
                    continue
                w = item.get("weight")
                # 일부 응답은 그램 단위일 수 있어 보정
                if isinstance(w, (int, float)) and w > 1000:
                    w = w / 1000.0
                if isinstance(w, (int, float)):
                    exist.add(round(float(w), 1))
        return exist
    except Exception as e:
        print(f"⚠️ 오늘 체중 조회 실패(중복 검사 건너뜀): {e}")
        return set()


def upload_rows(api: Garmin, rows: list[BodyRow], dry_run: bool, skip_duplicates: bool) -> tuple[int, int]:
    ok, fail = 0, 0
    for i, row in enumerate(rows, start=1):
        date_only = row.ts_iso.split("T")[0]
        duplicates = fetch_today_weights_map(api, date_only) if skip_duplicates else set()

        will_skip = skip_duplicates and round(row.weight, 1) in duplicates
        msg_head = f"[{i}/{len(rows)}] {row.ts_iso}  {row.weight} kg"
        if will_skip:
            print(f"{msg_head} → 이미 같은 날짜에 동일 체중 존재: 업로드 생략")
            continue

        if dry_run:
            print(f"{msg_head} (DRY-RUN) add_body_composition 호출 예정")
            ok += 1
            continue

        # 라이브러리 시그니처는 demo.py 기준:
        # api.add_body_composition(timestamp_iso, weight=..., percent_fat=..., percent_hydration=..., bone_mass=..., muscle_mass=..., basal_met=..., bmi=...)
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
            print(f"{msg_head} → ✅ 업로드 성공")
            ok += 1
            # 과도한 연속 호출 방지
            time.sleep(0.3)
        except Exception as e:
            print(f"{msg_head} → ❌ 업로드 실패: {e}")
            fail += 1
    return ok, fail


def main():
    ap = argparse.ArgumentParser(description="Upload Google Fit weight CSVs to Garmin Body Composition")
    ap.add_argument("--email", help="Garmin email (또는 환경변수 GARMIN_EMAIL 사용)")
    ap.add_argument("--password", help="Garmin password (또는 환경변수 GARMIN_PASSWORD 사용)")
    ap.add_argument("--csv", nargs="*", default=[], help="CSV 파일 경로 혹은 글롭 패턴 (여러 개 가능). 미지정 시 최신 *.csv 자동 선택")
    ap.add_argument("--no-skip-duplicates", action="store_true", help="같은 날짜 동일 체중이어도 업로드 강행")
    ap.add_argument("--dry-run", action="store_true", help="실제 업로드 대신 시뮬레이션만 수행")
    args = ap.parse_args()

    # CSV 선택
    targets: list[str] = []
    if args.csv:
        for pat in args.csv:
            matches = glob.glob(pat)
            targets.extend([m for m in matches if m.lower().endswith(".csv")])
    else:
        latest = choose_latest_csv(["*.csv", "**/*.csv"])
        if latest:
            targets = [latest]

    if not targets:
        print("❌ 업로드할 CSV를 찾지 못했습니다. --csv 패턴을 지정하거나 작업 디렉토리에 CSV를 두세요.")
        sys.exit(2)

    print("📄 대상 CSV:")
    for p in targets:
        ts = datetime.fromtimestamp(os.path.getmtime(p)).strftime("%Y-%m-%d %H:%M:%S")
        print(f"  - {p} (mtime: {ts})")

    # 로그인
    api = login(args.email, args.password)

    # 로딩 & 업로드
    total_ok, total_fail = 0, 0
    for path in targets:
        try:
            rows = load_rows_from_csv(path)
            if not rows:
                print(f"ℹ️ {path}: 업로드할 행이 없습니다(몸무게 결측 등).")
                continue
            print(f"➡️  {path}: {len(rows)}개 행 처리 시작")
            ok, fail = upload_rows(
                api,
                rows,
                dry_run=args.dry_run,
                skip_duplicates=not args.no_skip_duplicates,
            )
            total_ok += ok
            total_fail += fail
        except Exception as e:
            print(f"❌ {path} 처리 실패: {e}")
            total_fail += 1

    print(f"\n[DONE] 업로드 성공: {total_ok}, 실패: {total_fail}, dry-run: {args.dry_run}")


if __name__ == "__main__":
    main()
