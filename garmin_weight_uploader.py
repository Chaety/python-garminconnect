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
    # 1) 토큰 복구 시도
    try:
        garth.resume(token_dir)
        # 간단 검증: username 접근이 되면 토큰 정상
        _ = garth.client.username
        print(f"[INFO] Logged in as: {garth.client.username}")
        return
    except FileNotFoundError:
        print("[INFO] No saved tokens. Will login fresh...")
    except Exception as e:
        print(f"[WARN] Resume failed ({e}). Will login fresh...")

    # 2) 로그인 & 토큰 저장
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
                mtime = os.path.getmtime(path)
                candidates.append((mtime, path))
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
    cols = df.columns.tolist()
    print(f"[INFO] Columns: {cols}")

    # 필수 컬럼 체크
    for req in ["날짜", "시간", "몸무게"]:
        if req not in df.columns:
            raise ValueError(f"CSV에 '{req}' 컬럼이 없습니다.")

    # 가공: 날짜 문자열 -> date, 시간 문자열 -> time, 몸무게 -> float(kg)
    # 날짜 컬럼이 'YYYY.MM.DD HH:MM:SS' 같이 들어올 수 있어 분리 처리
    def parse_date(s: str) -> datetime.date:
        s = str(s).strip()
        # 'YYYY.MM.DD HH:MM:SS' 형태면 공백 전까지만 사용
        s = s.split()[0]
        return datetime.strptime(s, "%Y.%m.%d").date()

    def parse_time(s: str) -> datetime.time:
        s = str(s).strip()
        # 'HH:MM:SS' 가정
        return datetime.strptime(s, "%H:%M:%S").time()

    # 안전 변환
    dates = []
    times = []
    weights = []
    for i, row in df.iterrows():
        try:
            d = parse_date(row["날짜"])
        except Exception:
            # 혹시 '날짜'에 날짜+시간 두 번 들어간 형태면 앞부분만 재시도
            d = parse_date(str(row["날짜"]).split()[0])
        try:
            t = parse_time(row["시간"])
        except Exception:
            # 시간 파싱 실패 시 00:00:00
            t = datetime.strptime("00:00:00", "%H:%M:%S").time()
        try:
            w = float(str(row["몸무게"]).replace('"', '').strip())
        except Exception:
            w = 0.0
        dates.append(d)
        times.append(t)
        weights.append(w)

    out = pd.DataFrame({"date": dates, "time": times, "weight_kg": weights})
    # 0이거나 음수는 제외
    out = out[out["weight_kg"] > 0].copy()
    if out.empty:
        raise ValueError("유효한(>0) 몸무게 값이 없습니다.")
    return out


def pick_last_per_date(df: pd.DataFrame) -> pd.DataFrame:
    """
    동일 날짜에서 가장 늦은 시간(최신 측정)만 선택
    """
    # 정렬 후 groupby().tail(1)
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
        # 참고: 필요시 소스 표기
        # "sourceType": "USER_ENTERED"
    }
    # garth.connectapi는 인증/토큰 자동 처리. method/json은 requests.request 스타일.
    resp = garth.connectapi(WEIGHT_POST_PATH, m
