#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
garmin_weight_uploader.py
- Google Fit CSV(무게 기록)에서 체중을 읽어 Garmin Connect에 업로드
- garth 0.5.x 이상 대응: tokenstore 제거, load_token/save_token 사용
- garminconnect 라이브러리 버전별 차이를 감안하여 여러 메서드명을 순차 시도
- 실패 시 garth HTTP API 직접 호출 시도
"""

import csv
import glob
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from garth import Garth

try:
    from garminconnect import Garmin  # type: ignore
except Exception:
    Garmin = None  # 없는 경우도 있음


def log(msg: str) -> None:
    print(msg, flush=True)


def die(msg: str) -> None:
    print(f"[FATAL] {msg}", file=sys.stderr, flush=True)
    sys.exit(1)


@dataclass
class WeightRecord:
    dt: datetime
    weight_kg: float


def ensure_login() -> Garth:
    email = os.environ.get("GARMIN_EMAIL")
    password = os.environ.get("GARMIN_PASSWORD")
    if not email or not password:
        die("GARMIN_EMAIL / GARMIN_PASSWORD 환경변수가 필요합니다.")

    g = Garth()
    try:
        g.load_token()
        g.refresh_token()
        log("[INFO] 토큰 재사용 로그인 성공")
    except Exception:
        log("[INFO] 신규 로그인 시도")
        g.login(email=email, password=password)
        g.save_token()
        log("[INFO] 로그인 & 토큰 저장 완료 (~/.garth/token.json)")

    return g


def find_latest_csv(patterns=("*Google Fit.csv", "무게 *.csv")) -> Optional[str]:
    files = []
    for p in patterns:
        files.extend(glob.glob(p))
    if not files:
        return None
    files.sort(key=lambda f: os.path.getmtime(f), reverse=True)
    return files[0]


def parse_weight_from_csv(path: str) -> Optional[WeightRecord]:
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        rows = list(reader)

    if not rows or len(rows) < 2:
        log(f"[WARN] CSV 데이터가 비어있습니다: {path}")
        return None

    header = rows[0]
    data = rows[1]

    def idx(colname_candidates):
        for name in colname_candidates:
            if name in header:
                return header.index(name)
        return -1

    i_date = idx(["날짜", "Date", "date"])
    i_time = idx(["시간", "Time", "time"])
    i_weight = idx(["몸무게", "Body Weight", "Weight", "weight"])

    if i_weight < 0:
        log("[WARN] '몸무게/Weight' 열을 찾을 수 없습니다.")
        return None

    raw_weight = (data[i_weight] or "").replace('"', "").strip()
    if not raw_weight:
        log("[WARN] 몸무게 값이 비어있습니다.")
        return None

    try:
        weight_kg = float(raw_weight)
    except Exception:
        log(f"[WARN] 몸무게 수치 파싱 실패: {raw_weight}")
        return None

    if i_date >= 0 and i_time >= 0:
        raw_date = (data[i_date] or "").replace('"', "").strip()
        raw_time = (data[i_time] or "").replace('"', "").strip()
        dt_str = raw_date
        if len(raw_date) <= 10 and raw_time:
            dt_str = f"{raw_date} {raw_time}"
        fmts = [
            "%Y.%m.%d %H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
            "%Y.%m.%d",
            "%Y-%m-%d",
        ]
        parsed = None
        for fmt in fmts:
            try:
                parsed = datetime.strptime(dt_str, fmt)
                break
            except Exception:
                pass
        dt = parsed or datetime.now()
    else:
        dt = datetime.now()

    return WeightRecord(dt=dt, weight_kg=weight_kg)


def try_upload_with_garminconnect(garth_cli: Garth, rec: WeightRecord) -> bool:
    if Garmin is None:
        return False

    api = None
    for kwargs in ({"garth": garth_cli}, {"client": garth_cli}, {}):
        try:
            api = Garmin(**kwargs)  # type: ignore
            break
        except TypeError:
            continue
        except Exception:
            continue

    if api is None:
        email = os.environ.get("GARMIN_EMAIL")
        password = os.environ.get("GARMIN_PASSWORD")
        if not email or not password:
            return False
        try:
            api = Garmin(email, password)  # type: ignore
            api.login()  # type: ignore
        except Exception as e:
            log(f"[WARN] garminconnect 구버전 로그인 실패: {e}")
            api = None

    if api is None:
        return False

    candidates = [
        ("set_bodyweight", {"weight": rec.weight_kg, "date": rec.dt.date()}),
        ("set_weight", {"weight": rec.weight_kg, "date": rec.dt.date()}),
        ("upload_weight", {"weight": rec.weight_kg, "date": rec.dt.date()}),
    ]

    for method_name, kwargs in candidates:
        try:
            m = getattr(api, method_name, None)
            if callable(m):
                m(**kwargs)  # type: ignore
                log(f"[INFO] garminconnect.{method_name}() 업로드 성공: {rec.weight_kg} kg @ {rec.dt}")
                return True
        except Exception as e:
            log(f"[WARN] {method_name} 실패: {e}")

    return False


def try_upload_with_garth_http(garth_cli: Garth, rec: WeightRecord) -> bool:
    try:
        payload = {
            "value": rec.weight_kg,
            "unit": "kg",
            "sourceType": "MANUAL",
            "timestamp": int(rec.dt.timestamp() * 1000),
        }
        r = garth_cli.connectjson(
            method="POST",
            url="https://connect.garmin.com/modern/proxy/weight-service/user-weight",
            json=payload,
        )
        if r and (isinstance(r, dict) or isinstance(r, list)):
            log("[INFO] garth HTTP 업로드 성공(POST user-weight)")
            return True
    except Exception as e:
        log(f"[WARN] garth HTTP(POST) 실패: {e}")

    try:
        epoch_ms = int(rec.dt.timestamp() * 1000)
        payload = {"value": rec.weight_kg, "unit": "kg", "sourceType": "MANUAL"}
        r = garth_cli.connectjson(
            method="PUT",
            url=f"https://connect.garmin.com/modern/proxy/weight-service/user-weight/{epoch_ms}",
            json=payload,
        )
        if r is None or isinstance(r, (dict, list)):
            log("[INFO] garth HTTP 업로드 성공(PUT user-weight/{ts})")
            return True
    except Exception as e:
        log(f"[WARN] garth HTTP(PUT) 실패: {e}")

    return False


def run() -> None:
    log("[START] weight uploader")
    garth_cli = ensure_login()

    csv_path = find_latest_csv()
    if not csv_path:
        die("CSV 파일을 찾지 못했습니다.")

    rec = parse_weight_from_csv(csv_path)
    if not rec:
        die(f"CSV 파싱 실패: {csv_path}")

    log(f"[INFO] 업로드 대상: {rec.weight_kg} kg @ {rec.dt}")

    uploaded = try_upload_with_garminconnect(garth_cli, rec)
    if not uploaded:
        log("[WARN] garminconnect 실패 → garth HTTP 시도")
        uploaded = try_upload_with_garth_http(garth_cli, rec)

    if not uploaded:
        die("업로드 실패 (라이브러리/엔드포인트 변경 가능성)")

    log("[DONE] 업로드 성공")


if __name__ == "__main__":
    try:
        run()
    except SystemExit:
        raise
    except Exception as e:
        print(f"[FATAL] 예외: {e}", file=sys.stderr, flush=True)
        raise
