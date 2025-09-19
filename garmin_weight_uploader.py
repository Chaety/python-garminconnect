# --- add imports (상단) ---
from pathlib import Path
import pandas as pd
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")

# 가민 조회/업로드 경로 (기존 파일에 이미 있다면 그대로 사용)
WEIGHT_POST_PATH = "/connectapi/weight/weight"           # 기존과 동일
WEIGHT_RANGE_GET_PATH = "/connectapi/weight/weight/dateRange"  # 일자 범위 조회

# --- helper: 구글핏 CSV 파싱 ---
def parse_googlefit_weight_csv(csv_path: Path) -> pd.DataFrame:
    """
    csv 예시 헤더:
    날짜,시간,몸무게,체지방률,...
    2025.09.18 00:00:00,00:00:00,"70.79891",...
    """
    df = pd.read_csv(csv_path)
    # 컬럼 명 매핑(한글/공백 포함 대비)
    colmap = {
        "날짜": "date_str",
        "시간": "time_str",
        "몸무게": "weight",
        "kst": "kst",  # 혹시 kst라는 컬럼명이 따로 오는 경우 대비
    }
    for k, v in colmap.items():
        if k in df.columns:
            df.rename(columns={k: v}, inplace=True)

    # KST 기준 timestamp 만들기
    ts = []
    for i, row in df.iterrows():
        # 가장 신뢰도 높은 순서로 선택
        if "kst" in df.columns and pd.notna(row.get("kst")):
            # kst가 "YYYY.MM.DD HH:MM:SS" 또는 ISO 형태라고 가정
            s = str(row["kst"]).replace(".", "-")
            s = s.replace(" ", "T") if "T" not in s else s
            try:
                dt = datetime.fromisoformat(s)
            except Exception:
                dt = datetime.strptime(str(row["kst"]), "%Y.%m.%d %H:%M:%S")
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=KST)
        else:
            # "날짜"+"시간" 조합
            date_str = str(row.get("date_str", "")).strip()
            time_str = str(row.get("time_str", "00:00:00")).strip()
            if date_str and "." in date_str:
                # "YYYY.MM.DD HH:MM:SS" 형태로 합치기
                if " " in date_str:
                    # 이미 시간 포함된 경우
                    base = date_str
                else:
                    base = f"{date_str} {time_str}"
                dt = datetime.strptime(base, "%Y.%m.%d %H:%M:%S").replace(tzinfo=KST)
            else:
                # 날짜가 없으면 업로드 시각(now, KST)
                dt = datetime.now(tz=KST)

        ts.append(dt)

    df["dt_kst"] = ts

    # 몸무게 정규화 (문자열/따옴표 제거 → float)
    df["weight"] = pd.to_numeric(df["weight"], errors="coerce")
    df = df.dropna(subset=["weight"])

    # (timestamp, weight) 기준 중복 제거
    df = df.drop_duplicates(subset=["dt_kst", "weight"]).sort_values("dt_kst")

    # 가민에 보낼 필드 준비
    df["date"] = df["dt_kst"].dt.strftime("%Y-%m-%d")
    df["time"] = df["dt_kst"].dt.strftime("%H:%M:%S")
    return df[["dt_kst", "date", "time", "weight"]]


# --- helper: 가민에 이미 있는 날짜 수집 ---
def fetch_existing_weight_dates(garth) -> set[str]:
    """
    최근 2년 정도 범위를 한 번에 조회(필요 시 조정 가능).
    응답에서 기록이 존재하는 날짜들을 set으로 반환.
    """
    today = datetime.now(tz=KST).date()
    start = today.replace(year=today.year - 2)
    params = {
        "startDate": start.isoformat(),
        "endDate": today.isoformat(),
    }
    resp = garth.connectapi(WEIGHT_RANGE_GET_PATH, params=params, method="GET")
    # 응답 형태 예시 가정:
    # [{"date":"2025-09-18","weight":69.2,...}, {"date":"2025-09-19","weight":69.8,...}, ...]
    existing = set()
    if isinstance(resp, list):
        for item in resp:
            d = item.get("date")
            if d:
                existing.add(d)
    return existing


# --- helper: 업로드 (기존 로직 재사용) ---
def upload_weight(garth, date_str: str, time_str: str, weight_kg: float):
    """
    가민 단건 업로드. 기존 파일에서 사용하던 payload/경로를 유지.
    """
    payload = {
        "date": date_str,             # YYYY-MM-DD
        "time": time_str,             # HH:MM:SS (local)
        "weight": float(weight_kg),   # kg
        "unitKey": "kg",
        "sourceType": "USER_ENTERED",
        "timeZone": "Asia/Seoul",
    }
    # 기존 코드와 동일하게 POST
    garth.connectapi(WEIGHT_POST_PATH, method="POST", json=payload)


# --- main 업로드 흐름(핵심) ---
def run_upload_all_csv(garth):
    # 1) 가민에 이미 기록된 날짜 미리 조회 → 그 날짜는 건너뜀
    existing_dates = fetch_existing_weight_dates(garth)

    # 2) 작업 폴더의 모든 CSV 순회(구글 드라이브에서 rclone으로 받아온 파일들)
    csv_paths = sorted(Path(".").glob("*.csv"))
    if not csv_paths:
        print("[INFO] CSV가 없습니다. 종료합니다.")
        return

    total, skipped_dates, uploaded = 0, set(), 0

    for csv_path in csv_paths:
        df = parse_googlefit_weight_csv(csv_path)
        if df.empty:
            continue

        # 이 CSV에 포함된 날짜 집합
        dates_in_csv = sorted(df["date"].unique())

        # 날짜 단위 스킵 규칙
        # → 이 CSV의 각 날짜가 existing_dates에 있으면 해당 날짜의 레코드 전부 스킵
        for date_str in dates_in_csv:
            if date_str in existing_dates:
                skipped_dates.add(date_str)
                continue

            # 날짜별 레코드 업로드
            rows = df[df["date"] == date_str]
            for _, r in rows.iterrows():
                # time이 비어있을 수 있는 경우 now(KST)로 대체
                time_str = r.get("time") or datetime.now(tz=KST).strftime("%H:%M:%S")
                weight = float(r["weight"])
                upload_weight(garth, date_str, time_str, weight)
                uploaded += 1
            # 업로드가 성공했다면, 해당 날짜는 다시 업로드하지 않도록 메모
            existing_dates.add(date_str)

        total += len(df)

    print(f"[INFO] 총 CSV 레코드: {total}")
    if skipped_dates:
        print(f"[INFO] 가민에 이미 존재해서 스킵된 날짜: {', '.join(sorted(skipped_dates))}")
    print(f"[OK] 업로드 건수: {uploaded}")
