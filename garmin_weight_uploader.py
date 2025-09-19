from pathlib import Path
import pandas as pd
from datetime import datetime, date
from zoneinfo import ZoneInfo
import os
import sys
import traceback

from garth import Client as Garth  # 토큰 캐시 사용
# garminconnect는 설치만 되어 있으면 됩니다.

KST = ZoneInfo("Asia/Seoul")

WEIGHT_POST_PATH = "/connectapi/weight/weight"
WEIGHT_RANGE_GET_PATH = "/connectapi/weight/weight/dateRange"

def log(msg: str):
    print(msg, flush=True)

def parse_googlefit_weight_csv(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path)

    # 컬럼 리네임
    colmap = {"날짜": "date_str", "시간": "time_str", "몸무게": "weight", "kst": "kst"}
    for k, v in colmap.items():
        if k in df.columns:
            df.rename(columns={k: v}, inplace=True)

    # KST 타임스탬프 생성
    ts = []
    for _, row in df.iterrows():
        dt = None
        kst_val = row.get("kst")
        if pd.notna(kst_val):
            s = str(kst_val).strip()
            # 가능한 포맷들 보수적으로 처리
            try:
                # "YYYY.MM.DD HH:MM:SS"
                dt = datetime.strptime(s, "%Y.%m.%d %H:%M:%S").replace(tzinfo=KST)
            except Exception:
                try:
                    # ISO 류
                    s2 = s.replace(".", "-")
                    if "T" not in s2 and " " in s2:
                        s2 = s2.replace(" ", "T")
                    dt = datetime.fromisoformat(s2)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=KST)
                except Exception:
                    dt = None

        if dt is None:
            date_str = str(row.get("date_str") or "").strip()
            time_str = str(row.get("time_str") or "00:00:00").strip()
            if date_str:
                if " " in date_str:
                    base = date_str
                else:
                    base = f"{date_str} {time_str}"
                try:
                    dt = datetime.strptime(base, "%Y.%m.%d %H:%M:%S").replace(tzinfo=KST)
                except Exception:
                    dt = datetime.now(tz=KST)
