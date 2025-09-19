from __future__ import annotations
from pathlib import Path
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import os, sys, traceback
import pandas as pd
from garth import Client as Garth  # OAuth 토큰/세션

KST = ZoneInfo("Asia/Seoul")
WEIGHT_POST_PATH = "/connectapi/weight/weight"
WEIGHT_RANGE_GET_PATH = "/connectapi/weight/weight/dateRange"

def log(msg: str): print(msg, flush=True)

def _to_kst_dt(date_str: str|None, time_str: str|None) -> datetime|None:
    if not date_str and not time_str: return None
    # 포맷들 방어적으로 처리
    ds = (date_str or "").strip()
    ts = (time_str or "").strip()
    # “YYYY.MM.DD HH:MM:SS” 가 날짜 컬럼에 통째로 들어오기도 함
    if ds and (" " in ds and ":" in ds) and not ts:
        try: return datetime.strptime(ds, "%Y.%m.%d %H:%M:%S").replace(tzinfo=KST)
        except: pass
    # 일반 케이스
    if not ts: ts = "00:00:00"
    for fmt in ("%Y.%m.%d %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try: return datetime.strptime(f"{ds} {ts}", fmt).replace(tzinfo=KST)
        except: pass
    return None

def parse_googlefit_csv(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    # 컬럼 정규화(있는 것만)
    rename = {"날짜":"date","시간":"time","몸무게":"weight","kst":"kst"}
    for k,v in rename.items():
        if k in df.columns: df.rename(columns={k:v}, inplace=True)
    # 체중 숫자화
    df["weight"] = pd.to_numeric(df.get("weight"), errors="coerce")
    df = df.dropna(subset=["weight"]).copy()

    # 우선순위: kst → (날짜,시간) → 업로드시각(나중에 대체)
    dts = []
    for _, r in df.iterrows():
        dt = None
        if "kst" in df.columns and pd.notna(r.get("kst")):
            s = str(r.get("kst")).strip()
            # "YYYY.MM.DD HH:MM:SS" / ISO 유사 처리
            try:
                dt = datetime.strptime(s, "%Y.%m.%d %H:%M:%S").replace(tzinfo=KST)
            except:
                try:
                    s2 = s.replace(".", "-")
                    if "T" not in s2 and " " in s2: s2 = s2.replace(" ", "T")
                    dt = datetime.fromisoformat(s2)
                    if dt.tzinfo is None: dt = dt.replace(tzinfo=KST)
                except: dt = None
        if dt is None:
            dt = _to_kst_dt(str(r.get("date") or ""), str(r.get("time") or ""))
        dts.append(dt)
    df["dt_kst"] = dts

    # 동일 타임스탬프+체중 완전 중복 제거(같은 CSV 안)
    df = df.drop_duplicates(subset=["dt_kst","weight"])
    # 업로드 시각이 필요한 행은 표시(실제 업로드 때 NOW로 채움)
    df["needs_now"] = df["dt_kst"].isna()
    return df[["dt_kst","weight","needs_now"]].sort_values("dt_kst", na_position="last").reset_index(drop=True)

def ensure_login() -> Garth:
    email = os.environ.get("GARMIN_EMAIL")
    password = os.environ.get("GARMIN_PASSWORD")
    if not email or not password:
        print("[FATAL] GARMIN_EMAIL/GARMIN_PASSWORD 누락", file=sys.stderr); sys.exit(1)
    token_dir = Path.home()/".garminconnect"
    token_dir.mkdir(parents=True, exist_ok=True)
    garth = Garth(tokenstore=str(token_dir))
    try:
        garth.refresh_oauth2_token()
        log("[INFO] 토큰 재사용 로그인 성공")
    except:
        log("[INFO] 신규 로그인 시도")
        garth.login(email=email, password=password)
        log("[INFO] 로그인 성공")
    return garth

def fetch_existing(garth: Garth, start: datetime, end: datetime) -> set[tuple[str,str,float]]:
    """가민에 이미 있는 (date,time,weight) 셋. time이 없게 내려오는 경우가 있어
    최소한 (date, weight)이 같으면 중복으로 간주."""
    existing: set[tuple[str,str,float]] = set()
    try:
        params = {"startDate": start.date().isoformat(), "endDate": end.date().isoformat()}
        data = garth.connectapi(WEIGHT_RANGE_GET_PATH, params=params, method="GET")
        if isinstance(data, list):
            for it in data:
                d = str(it.get("date") or "")
                t = str(it.get("time") or "")
                w = float(it.get("weight")) if it.get("weight") is not None else None
                if d and w is not None:
                    # 가민 웹은 소수 0.1 단위 반올림되어 보이기도 → 0.01 단위로 라운드
                    existing.add((d, t, round(w, 2)))
        log(f"[INFO] 기존 기록 {len(existing)}건 조회")
    except Exception as e:
        log(f"[WARN] 기존 기록 조회 실패: {e} (중복체크 약화)")
    return existing

def post_weight(garth: Garth, date_str: str, time_str: str, w: float):
    payload = {
        "date": date_str,
        "time": time_str,
        "weight": float(w),
        "unitKey": "kg",
        "sourceType": "USER_ENTERED",
        "timeZone": "Asia/Seoul",
    }
    return garth.connectapi(WEIGHT_POST_PATH, method="POST", json=payload)

def run():
    log("[START] weight uploader")
    garth = ensure_login()

    csvs = sorted(Path(".").glob("*.csv"))
    if not csvs:
        log("[INFO] CSV 없음"); return

    # CSV 범위로 기존 데이터 가져오기
    # (최소/최대 날짜 추정; dt가 없는 행도 있을 수 있어 30일 범위로 보수)
    min_dt, max_dt = None, None
    parsed_per_file = []
    for p in csvs:
        df = parse_googlefit_csv(p)
        parsed_per_file.append((p, df))
        dts = df["dt_kst"].dropna()
        if not dts.empty:
            mn, mx = dts.min(), dts.max()
            min_dt = mn if (min_dt is None or mn < min_dt) else min_dt
            max_dt = mx if (max_dt is None or mx > max_dt) else max_dt
    if min_dt is None:
        min_dt = datetime.now(tz=KST) - timedelta(days=30)
    if max_dt is None:
        max_dt = datetime.now(tz=KST)

    existing = fetch_existing(garth, min_dt, max_dt)

    uploaded = 0
    for p, df in parsed_per_file:
        log(f"[FILE] {p.name} — rows:{len(df)}")
        for _, r in df.iterrows():
            dt: datetime|None = r["dt_kst"]
            w = round(float(r["weight"]), 2)

            if dt is None:
                # 시간 정보가 없으면 업로드 시각 사용(KST)
                dt = datetime.now(tz=KST)

            dstr = dt.strftime("%Y-%m-%d")
            tstr = dt.strftime("%H:%M:%S")

            # 가민에 이미 같은 (date,time,weight) 또는 (date,'',weight) 있으면 스킵
            key_exact = (dstr, tstr, w)
            key_date_only = (dstr, "", w)
            if key_exact in existing or key_date_only in existing:
                log(f"[SKIP] 이미 존재: {dstr} {tstr} {w}kg")
                continue

            try:
                post_weight(garth, dstr, tstr, w)
                uploaded += 1
                existing.add(key_exact)  # 즉시 중복 방지
                log(f"[OK]   {dstr} {tstr} -> {w}kg")
            except Exception as e:
                log(f"[ERR]  {dstr} {tstr} -> {w}kg : {e}")
                traceback.print_exc()

    log(f"[DONE] 업로드 {uploaded}건 완료")

if __name__ == "__main__":
    try:
        run()
    except SystemExit:
        raise
    except Exception as e:
        log(f"[FATAL] 예외: {e}")
        traceback.print_exc()
        sys.exit(1)
