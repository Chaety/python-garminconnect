#!/usr/bin/env python3
# garmin_weight_uploader.py
# - Google Drive에서 받은 CSV(예: "무게 YYYY.MM.DD Google Fit.csv")를 읽어
#   날짜별 마지막 유효 몸무게만 Garmin Connect에 업로드합니다.
# - garth 0.5.x 기준: 세션/쿠키는 garth가 내부에서 관리하며,
#   API 호출은 garth.connectapi()로 수행합니다.

import os
import sys
import glob
import json
from datetime import datetime
from typing import List, Tuple, Optional

import pandas as pd
import garth


TOKEN_DIR = os.path.expanduser("~/.garminconnect")  # actions/cache와 동일 경로 사용
WEIGHT_POST_PATH = "weight-service/user-weight"     # modern/proxy/ 접두사는 garth가 알아서 붙임


def ensure_login(email: str, password: str, token_dir: str = TOKEN_DIR) -> None:
    """
    garth 토큰을 복구(resume)하거나, 없으면 로그인 후 저장(save).
    MFA가 설정돼 있으면 garth가 프롬프트로 코드를 받습니다.
    """
    try:
        garth.resume(token_dir)
        _ = garth.client.username  # 토큰 유효성 확인
        print(f"[INFO] Logged in as: {garth.client.username}")
        return
    except FileNotFoundError:
        print("[INFO] No saved tokens. Will login fresh...")
    except Exception as e:
        print(f"[WARN] Resume failed ({e}). Will login fresh...")

    garth.login(email, password)
    garth.save(token_dir)
    print(f"[INFO] Login success. Saved tokens to {token_dir}. User: {garth.client.username}")


def find_latest_csv(patterns: List[str]) -> Optional[str]:
    """
    지정 패턴 목록 중 가장 최근 mtime의 파일을 반환.
    """
    candidates: List[Tuple[float, str]] = []
    for pat in patterns:
        for path in glob.glob(pat):
            try:
                candidates.append((os.path.getmtime(path), path))
            except OSError:
                pass
    if not candidates:
        return None
    candidates.sort(key=lambda t: t[0], reverse=True)
    return candidates[0][1]


def load_weight_rows(csv_path: str) -> pd.DataFrame:
    """
    한국어 헤더의 CSV를 읽어 필요한 컬럼을 표준화.
    예상 헤더:
      날짜,시간,몸무게,체지방률,체지방량,무지방 비율,무지방 질량,골격근 비율,골격근량,근육량 비율,근육량,골량,총 체수분,기본 대사율
    """
    print(f"[INFO] Selected CSV: {csv_path}")
    df = pd.read_csv(csv_path, encoding="utf-8-sig")

    for req in ["날짜", "시간", "몸무게"]:
        if req not in df.columns:
            raise ValueError(f"CSV에 '{req}' 컬럼이 없습니다.")

    def parse_date(s: str) -> datetime.date:
        s = str(s).strip().split()[0]  # 'YYYY.MM.DD HH:MM:SS' -> 날짜만
        return datetime.strptime(s, "%Y.%m.%d").date()

    def parse_time(s: str) -> datetime.time:
        s = str(s).strip()
        return datetime.strptime(s, "%H:%M:%S").time()

    rows = []
    for _, row in df.iterrows():
        try:
            d = parse_date(row["날짜"])
        except Exception:
            d = parse_date(str(row["날짜"]).split()[0])
        try:
            t = parse_time(row["시간"])
        except Exception:
            t = datetime.strptime("00:00:00", "%H:%M:%S").time()
        try:
            w = float(str(row["몸무게"]).replace('"', '').strip())
        except Exception:
            w = 0.0
        if w > 0:
            rows.append((d, t, w))

    if not rows:
        raise ValueError("유효한(>0) 몸무게 값이 없습니다.")

    out = pd.DataFrame(rows, columns=["date", "time", "weight_kg"])
    return out


def pick_last_per_date(df: pd.DataFrame) -> pd.DataFrame:
    """동일 날짜에서 가장 늦은 시간(최신 측정)만 선택"""
    df_sorted = df.sort_values(["date", "time"])
    last_rows = df_sorted.groupby("date", as_index=False).tail(1)
    return last_rows.sort_values("date")


def post_weight(date_str: str, kg: float) -> dict:
    """
    가민 Connect Weight API로 업로드.
    path: weight-service/user-weight  (garth가 modern/proxy/ 접두사를 처리)
    payload: {"value": <kg>, "unitKey": "kg", "date": "YYYY-MM-DD"}
    """
    payload = {
        "value": round(float(kg), 3),
        "unitKey": "kg",
        "date": date_str,
    }
    resp = garth.connectapi(
        WEIGHT_POST_PATH,
        method="POST",
        json=payload,
    )
    return resp


def main():
    email = os.getenv("GARMIN_EMAIL")
    password = os.getenv("GARMIN_PASSWORD")
    if not email or not password:
        print("[ERROR] 환경변수 GARMIN_EMAIL/GARMIN_PASSWORD 가 필요합니다.", file=sys.stderr)
        sys.exit(2)

    ensure_login(email, password, TOKEN_DIR)

    csv_path = find_latest_csv([
        "*Google Fit.csv",
        "*.csv",
    ])
    if not csv_path:
        print("[ERROR] 작업 폴더에서 CSV를 찾지 못했습니다.", file=sys.stderr)
        sys.exit(3)

    df = load_weight_rows(csv_path)
    target = pick_last_per_date(df)

    print("[INFO] Upload candidates:")
    for _, r in target.iterrows():
        print(f"  - {r['date'].isoformat()}  {r['time']}  {r['weight_kg']} kg")

    failures = 0
    for _, r in target.iterrows():
        date_str = r["date"].isoformat()
        kg = float(r["weight_kg"])
        try:
            resp = post_weight(date_str, kg)
            print(f"[OK] {date_str} -> {kg} kg  resp: {json.dumps(resp)[:200]}...")
        except Exception as e:
            failures += 1
            print(f"[FAIL] {date_str} -> {kg} kg  ({e})", file=sys.stderr)

    if failures:
        sys.exit(1)


if __name__ == "__main__":
    main()
