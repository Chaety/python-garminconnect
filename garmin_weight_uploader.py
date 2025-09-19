# -*- coding: utf-8 -*-
"""
Drive에서 받은 최신 CSV 1개를 읽어 Garmin Connect에 체중 업로드.
- 컬럼명(한글, 고정): 날짜(필수), 몸무게(필수), 시간(선택)
- 시간은 KST로 해석 후 UTC로 변환해 업로드 시도
- 순서:
    1) add_weigh_in_with_timestamps(weight, timestamp) 시도
    2) add_weigh_in(weight) 시도
    3) 위가 JSON 파싱 오류 등으로 실패하면, 인증 세션으로
       https://connect.garmin.com/modern/proxy/weight-service/user-weight
       에 직접 POST (204/200 등 2xx면 성공으로 처리)
- DRY_RUN=1 이면 업로드 대신 파싱 결과만 출력
"""

import os
import sys
from pathlib import Path
from datetime import timezone, timedelta
import pandas as pd
from dateutil import parser as dateparser

DATE_COL   = "날짜"
TIME_COL   = "시간"
WEIGHT_COL = "몸무게"
KST = timezone(timedelta(hours=9))

def pick_latest_csv() -> Path:
    files = sorted(Path(".").glob("*.csv"))
    if not files:
        print("[ERROR] *.csv not found in workspace")
        sys.exit(1)
    latest = max(files, key=lambda p: p.stat().st_mtime)
    print(f"[INFO] Selected CSV: {latest}")
    return latest

def read_csv(path: Path) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception:
            continue
    print("[ERROR] Failed to read CSV (utf-8-sig/utf-8/cp949 tried)")
    sys.exit(1)

def to_float(x):
    try:
        s = str(x).strip().replace(",", "").replace("%", "")
        return float(s) if s else None
    except Exception:
        return None

def parse_ts(date_val, time_val):
    if pd.isna(date_val) or str(date_val).strip() == "":
        return None
    text = str(date_val) if (time_val is None or str(time_val).strip() == "") else f"{date_val} {time_val}"
    dt = dateparser.parse(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=KST)
    return dt.astimezone(timezone.utc)

def _get_requests_session(garmin_obj):
    """
    garminconnect 버전마다 세션 경로가 달라서 최대한 안전하게 찾아옴.
    반환: requests.Session 또는 None
    """
    cand = [
        getattr(garmin_obj, "garth", None),
        getattr(garmin_obj, "client", None),
    ]
    for c in cand:
        if c is None:
            continue
        # garth.Client or wrapper
        sess = getattr(c, "session", None)
        if sess is not None and hasattr(sess, "request"):
            return sess
        # garth wrapper가 client 속성을 또 가질 때
        inner = getattr(c, "client", None)
        if inner is not None and hasattr(inner, "session"):
            sess2 = getattr(inner, "session")
            if hasattr(sess2, "request"):
                return sess2
    return None

def direct_post_weight(garmin_obj, date_yyyy_mm_dd: str, weight_kg: float) -> None:
    """
    라이브러리 함수가 JSON 파싱 오류로 실패하는 경우 직접 REST 호출.
    성공 시 예외 없이 종료. 비2xx면 예외 발생.
    """
    import json
    session = _get_requests_session(garmin_obj)
    if session is None:
        raise RuntimeError("auth session not found for direct POST")

    url = "https://connect.garmin.com/modern/proxy/weight-service/user-weight"
    payload = {
        "value": weight_kg,       # kg
        "unitKey": "kg",
        "sourceType": "MANUAL",
        "date": date_yyyy_mm_dd,  # 현지 날짜 (KST 기준 날짜 사용)
    }
    headers = {
        "Content-Type": "application/json",
        "NK": "NT",   # 일부 엔드포인트에서 필요
    }
    r = session.post(url, data=json.dumps(payload), headers=headers)
    if not (200 <= r.status_code < 300):
        raise RuntimeError(f"direct POST failed: {r.status_code} {r.text!r}")

def main():
    email = os.getenv("GARMIN_EMAIL")
    password = os.getenv("GARMIN_PASSWORD")
    dry_run = os.getenv("DRY_RUN", "0") == "1"

    csv_path = pick_latest_csv()
    df = read_csv(csv_path)
    print("[INFO] Columns:", list(df.columns))

    for col in (DATE_COL, WEIGHT_COL):
        if col not in df.columns:
            print(f"[ERROR] Missing column: {col}")
            sys.exit(1)

    from garminconnect import Garmin
    client = Garmin(email, password)
    client.login()
    print("[INFO] Garmin login OK")

    success, failed = 0, 0

    for i, row in df.iterrows():
        ts_utc = parse_ts(row.get(DATE_COL), row.get(TIME_COL) if TIME_COL in df.columns else None)
        weight = to_float(row.get(WEIGHT_COL))
        if ts_utc is None or weight is None:
            print(f"[SKIP] {i}: ts/weight missing -> {row.get(DATE_COL)} {row.get(TIME_COL)} {row.get(WEIGHT_COL)}")
            failed += 1
            continue

        if dry_run:
            print(f"[DRY] {i}: {ts_utc.isoformat()} UTC, {weight}kg")
            success += 1
            continue

        # 1) timestamp 지원 시도
        tried_ts = False
        try:
            if hasattr(client, "add_weigh_in_with_timestamps"):
                tried_ts = True
                client.add_weigh_in_with_timestamps(
                    weight=weight,
                    timestamp=ts_utc.isoformat(timespec="milliseconds"),
                )
                print(f"[OK] {i}: with timestamp -> {ts_utc.isoformat()} UTC, {weight}kg")
                success += 1
                continue
        except TypeError:
            # 시그니처 불일치 → 폴백
            pass
        except Exception as e:
            print(f"[WARN] with_timestamps failed: {e}")

        # 2) 폴백: weight만 업로드
        tried_plain = False
        try:
            tried_plain = True
            client.add_weigh_in(weight=weight)
            print(f"[OK] {i}: weight only -> {weight}kg")
            success += 1
            continue
        except Exception as e:
            print(f"[WARN] add_weigh_in failed: {e}")

        # 3) 라이브러리 둘 다 실패 → 직접 REST 호출
        try:
            # 가민 쪽 날짜는 현지 날짜 문자열이 가장 안전 (KST 기준 날짜)
            date_kst = ts_utc.astimezone(KST).strftime("%Y-%m-%d")
            direct_post_weight(client, date_kst, weight)
            # 직접 POST는 204(No Content) 등의 빈 응답이 정상 → 여기까지 오면 성공
            label = "direct (date only)" if not tried_ts else "direct (fallback)"
            print(f"[OK] {i}: {label} -> date={date_kst}, weight={weight}kg")
            success += 1
        except Exception as e:
            print(f"[FAIL] {i}: {e}")
            failed += 1

    print(f"Done. success={success}, failed={failed}")

if __name__ == "__main__":
    main()
