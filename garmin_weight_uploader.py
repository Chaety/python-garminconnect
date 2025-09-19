# --- 새 유틸 함수들 ----------------------------------------------------------
from datetime import datetime, timedelta, timezone
from collections import defaultdict
import csv
import os
import re

KOR_COLS = {
    "date": "날짜",
    "time": "시간",
    "weight": "몸무게",
}

ISO = "%Y-%m-%d"

def _parse_csv_daily_avg(csv_path: str) -> dict[str, float]:
    """
    CSV에서 같은 '날짜'별로 몸무게 평균을 계산해 { 'YYYY-MM-DD': 평균(kg) } 반환.
    """
    by_day = defaultdict(list)
    with open(csv_path, newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        for row in rdr:
            # 날짜 칸은 '2025.09.18 00:00:00' 형식 → 앞의 날짜만 사용
            raw = row.get(KOR_COLS["date"]) or ""
            m = re.match(r"(\d{4})\.(\d{2})\.(\d{2})", raw)
            if not m:
                continue
            day = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

            w = (row.get(KOR_COLS["weight"]) or "").replace('"', "")
            try:
                kg = float(w)
            except ValueError:
                continue
            if kg <= 0:
                continue

            by_day[day].append(kg)

    # 평균 내기
    daily_avg = {}
    for day, vals in by_day.items():
        daily_avg[day] = round(sum(vals) / len(vals), 2)
    return daily_avg


def _get_existing_dates(garth, start: str, end: str) -> set[str]:
    """
    지정 구간에 이미 가민에 저장된 체중 기록이 있는 날짜(YYYY-MM-DD) 집합을 반환.
    가민 Connect 내부 API는 문서화가 약해서, 가장 호환성 높은 dateRange 엔드포인트를 사용.
    """
    path = f"weight-service/weight/dateRange?startDate={start}&endDate={end}"
    data = garth.connectapi(path, None, method="GET") or []
    # 응답 예시는 [{"date":"2025-09-18","weight":69.2, ...}, ...] 형태가 흔함
    existing = set()
    for item in data:
        d = item.get("date") or item.get("calendarDate")
        if d:
            existing.add(d[:10])
    return existing


def _post_weight(garth, day: str, kg: float):
    """
    시간 없이 날짜/무게만 업로드 (표시는 00:00로 보임이 정상).
    """
    payload = {
        "date": day,
        "weight": kg,
        "unitKey": "kg",
    }
    # 공식 앱과 동일 경로 (No Content면 None 반환)
    return garth.connectapi("weight-service/user-weight", payload, method="POST")


# --- 메인 처리 흐름(평균 & 중복 방지) -----------------------------------------
def run_upload(csv_path: str, garth):
    daily_avg = _parse_csv_daily_avg(csv_path)
    if not daily_avg:
        print("[INFO] CSV에서 업로드 후보를 찾지 못했습니다.")
        return

    start = min(daily_avg.keys())
    end   = max(daily_avg.keys())

    existing = _get_existing_dates(garth, start, end)
    print("[INFO] 업로드 후보(평균):")
    for d in sorted(daily_avg.keys()):
        mark = "(이미 있음)" if d in existing else ""
        print(f"  - {d}  {daily_avg[d]} kg {mark}")

    uploaded = 0
    skipped  = 0
    for d in sorted(daily_avg.keys()):
        if d in existing:
            print(f"[SKIP] {d} 이미 등록되어 있어 건너뜁니다.")
            skipped += 1
            continue
        resp = _post_weight(garth, d, daily_avg[d])
        print(f"[OK] {d} -> {daily_avg[d]} kg  resp: {resp!r}")
        uploaded += 1

    print(f"[SUMMARY] 업로드 {uploaded}건, 스킵 {skipped}건")
