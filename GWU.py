#!/usr/bin/env python3
"""
garmin_weight_uploader.py

- Google Fit CSV 전체를 읽어 Garmin Connect에 업로드
- 시간 처리: CSV는 KST(+09:00)로 해석, 업로드는 UTC(Z)로 전송 → Garmin에서 현지시간으로 올바르게 표시
- 중복 제거: (날짜+시간+체중) 기준 (표시는 KST 기준)
- BMI 자동 계산 (신장 174.8cm 고정)
- '골격근량'이 있으면 muscle_mass로 우선 반영, 없으면 '근육량' 사용
"""

import argparse
import glob
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime

import pandas as pd
from dateutil import parser as dtparser
from garminconnect import Garmin
from zoneinfo import ZoneInfo

# ──────────────────────────────────────────────────────────────────────────────
# 설정
# ──────────────────────────────────────────────────────────────────────────────
TOKEN_DIR = os.path.expanduser("~/.garminconnect")

# 사용자 신장 (m)
USER_HEIGHT_M = 1.748
USER_HEIGHT_M2 = USER_HEIGHT_M ** 2  # BMI 계산에 사용

# CSV 헤더 매핑 (한글 → 내부 표준 키)
HEADER_MAP = {
    "날짜": "date",
    "시간": "time",
    "몸무게": "weight",
    "체지방률": "percent_fat",
    "총 체수분": "percent_hydration",
    "골량": "bone_mass",
    # 근육 관련: 두 가지가 들어올 수 있으니 모두 받는다
    "근육량": "muscle_mass",            # 일반 근육량
    "근육량 비율": "percent_muscle",
    "골격근량": "skeletal_muscle_mass", # 골격근량(있으면 우선 사용)
    "골격근 비율": "percent_skeletal_muscle",
    "기본 대사율": "basal_met",
    "BMI": "bmi",
}

# API로 보낼 때 사용할 수 있는 바디 컴포지션 항목 키(None은 미전송)
BODY_FIELDS = (
    "percent_fat",
    "percent_hydration",
    "bone_mass",
    "muscle_mass",     # ← 최종적으로 여기에 '골격근량' 또는 '근육량'을 매핑해 보냄
    "basal_met",
    "bmi",
)


@dataclass
class BodyRow:
    ts_iso_utc: str           # 업로드용(UTC, '...Z')
    date_str_kst: str         # 중복키/표시용(KST)
    time_str_kst: str         # 중복키/표시용(KST)
    weight: float
    percent_fat: float | None = None
    percent_hydration: float | None = None
    bone_mass: float | None = None
    muscle_mass: float | None = None  # API가 받는 muscle_mass 최종값
    basal_met: float | None = None
    bmi: float | None = None

    # 원본 보존용(로그 확인용)
    src_muscle_mass: float | None = None             # 근육량
    src_skeletal_muscle_mass: float | None = None    # 골격근량

    def dup_key(self) -> tuple[str, str, float]:
        # KST 표시 기준으로 중복 제거
        return (self.date_str_kst, self.time_str_kst, round(self.weight, 2))


# ──────────────────────────────────────────────────────────────────────────────
# 유틸
# ──────────────────────────────────────────────────────────────────────────────
def _coerce_float(x) -> float | None:
    """문자열/숫자를 float로 변환. 빈값/0/에러는 None."""
    try:
        s = str(x).strip().replace(",", ".").replace('"', "")
        if s == "" or s.lower() in {"nan", "none"}:
            return None
        v = float(s)
        return None if v == 0 else v
    except Exception:
        return None


def _parse_timestamp_kst(date_str: str, time_str: str | None) -> datetime:
    """
    'YYYY.MM.DD HH:MM:SS' 또는 유사 포맷을 Asia/Seoul 기준 aware datetime으로.
    CSV가 로컬시각(KST)라 가정하고 tzinfo 미지정 시 KST 부여.
    """
    s = date_str.strip()
    if time_str and " " not in s and "T" not in s:
        s = f"{s} {time_str.strip()}"
    # '2025.09.19' → '2025-09-19'
    s = s.replace(".", "-")
    dt = dtparser.parse(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZoneInfo("Asia/Seoul"))
    return dt.replace(microsecond=0)


def _format_kst_for_display(dt_kst: datetime) -> tuple[str, str]:
    """
    KST 시간으로 Garmin UI와 유사 포맷:
      - date:  MM/DD/YYYY
      - time:  h:mm am/pm (소문자)
    """
    date_s = dt_kst.strftime("%m/%d/%Y")
    time_s = dt_kst.strftime("%I:%M %p").lower().lstrip("0")
    return date_s, time_s


def _to_utc_iso_z(dt_kst: datetime) -> str:
    """
    KST aware datetime → UTC로 변환 → 'YYYY-MM-DDTHH:MM:SSZ' 문자열.
    (Garmin이 UTC로 저장 후 로컬로 보여주도록 보장)
    """
    dt_utc = dt_kst.astimezone(ZoneInfo("UTC"))
    iso = dt_utc.isoformat()
    if iso.endswith("+00:00"):
        iso = iso[:-6] + "Z"
    return iso


# ──────────────────────────────────────────────────────────────────────────────
# CSV 로딩
# ──────────────────────────────────────────────────────────────────────────────
def load_rows_from_csv(path: str) -> list[BodyRow]:
    df = pd.read_csv(path)
    df = _rename_headers(df)
    if "date" not in df or "weight" not in df:
        return []

    rows: list[BodyRow] = []
    for _, r in df.iterrows():
        date_val = str(r.get("date", "")).strip()
        time_val = str(r.get("time", "")).strip() if "time" in df else ""

        dt_kst = _parse_timestamp_kst(date_val, time_val if time_val else None)
        ts_iso_utc = _to_utc_iso_z(dt_kst)
        date_s_kst, time_s_kst = _format_kst_for_display(dt_kst)

        weight = _coerce_float(r.get("weight"))
        if weight is None:
            continue

        src_muscle_mass = _coerce_float(r.get("muscle_mass"))
        src_skeletal_muscle_mass = _coerce_float(r.get("skeletal_muscle_mass"))
        muscle_mass = src_skeletal_muscle_mass if src_skeletal_muscle_mass is not None else src_muscle_mass

        bmi_csv = _coerce_float(r.get("bmi"))
        bmi_auto = round(weight / USER_HEIGHT_M2, 1) if weight is not None else None
        bmi = bmi_csv if bmi_csv is not None else bmi_auto

        rows.append(
            BodyRow(
                ts_iso_utc=ts_iso_utc,
                date_str_kst=date_s_kst,
                time_str_kst=time_s_kst,
                weight=weight,
                percent_fat=_coerce_float(r.get("percent_fat")),
                percent_hydration=_coerce_float(r.get("percent_hydration")),
                bone_mass=_coerce_float(r.get("bone_mass")),
                muscle_mass=muscle_mass,
                basal_met=_coerce_float(r.get("basal_met")),
                bmi=bmi,
                src_muscle_mass=src_muscle_mass,
                src_skeletal_muscle_mass=src_skeletal_muscle_mass,
            )
        )
    return rows


def _rename_headers(df: pd.DataFrame) -> pd.DataFrame:
    new_cols = {}
    for c in df.columns:
        if c in HEADER_MAP:
            new_cols[c] = HEADER_MAP[c]
        else:
            new_cols[c] = c.lower()
    return df.rename(columns=new_cols)


# ──────────────────────────────────────────────────────────────────────────────
# Garmin 로그인 & 업로드
# ──────────────────────────────────────────────────────────────────────────────
def login(email: str | None, password: str | None) -> Garmin:
    email = email or os.getenv("GARMIN_EMAIL")
    password = password or os.getenv("GARMIN_PASSWORD")
    if not email or not password:
        sys.exit("GARMIN_EMAIL / GARMIN_PASSWORD 필요")

    api = Garmin(email, password)
    os.makedirs(TOKEN_DIR, exist_ok=True)

    # 1) 저장된 토큰으로 복원 시도 (로그인 스킵 → 429 방지 핵심)
    try:
        api.garth.load(TOKEN_DIR)
        _ = api.display_name  # 토큰 유효성 확인 (만료 시 예외 발생)
        print("✅ 토큰 재사용 성공 (로그인 스킵)")
        return api
    except Exception as e:  # noqa: BLE001
        print(f"ℹ️  저장된 토큰 없음 또는 만료 ({e}) → 새 로그인 시도")

    # 2) 새 로그인 후 토큰 저장
    try:
        api.login()
        api.garth.dump(TOKEN_DIR)
        print("✅ Garmin 새 로그인 성공 (토큰 저장됨)")
    except Exception as e:
        sys.exit(f"❌ 로그인 실패: {e}")

    return api


def upload_rows(api: Garmin, rows: list[BodyRow], dry_run: bool, skip_duplicates: bool) -> None:
    seen: set[tuple[str, str, float]] = set()

    for row in rows:
        k = row.dup_key()
        if skip_duplicates and k in seen:
            print(f"⏭️  {row.date_str_kst} {row.time_str_kst} {row.weight}kg → 중복 스킵")
            continue
        seen.add(k)

        mm_src = (
            "골격근량" if (row.src_skeletal_muscle_mass is not None)
            else ("근육량" if (row.src_muscle_mass is not None) else "없음")
        )
        print(f"➡️ {row.date_str_kst} {row.time_str_kst}  {row.weight}kg  "
              f"(muscle_mass: {row.muscle_mass} [{mm_src}], BMI: {row.bmi}) 업로드 중... → {row.ts_iso_utc}")

        if dry_run:
            continue

        try:
            payload = {"weight": row.weight}
            for f in BODY_FIELDS:
                v = getattr(row, f)
                if v is not None:
                    payload[f] = v

            api.add_body_composition(row.ts_iso_utc, **payload)
            print("   ✅ 성공")
        except Exception as e:
            print(f"   ❌ 실패: {e}")

        time.sleep(0.3)


# ──────────────────────────────────────────────────────────────────────────────
# 진입점
# ──────────────────────────────────────────────────────────────────────────────
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
