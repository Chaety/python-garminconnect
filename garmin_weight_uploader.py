import os
import sys
import pandas as pd
from datetime import datetime, timezone
from garminconnect import Garmin

# Garmin API endpoints
BASE_URL = "https://connectapi.garmin.com"
WEIGHT_POST_PATH = "userprofile-service/userprofile/weight"

def load_csv_latest():
    csv_files = [f for f in os.listdir(".") if f.endswith(".csv")]
    if not csv_files:
        print("[ERROR] CSV 파일이 없습니다.")
        sys.exit(1)

    latest_csv = max(csv_files, key=os.path.getmtime)
    print(f"[INFO] Selected CSV: {latest_csv}")

    df = pd.read_csv(latest_csv)
    print(f"[INFO] Columns: {list(df.columns)}")
    return df

def login_garmin():
    email = os.environ.get("GARMIN_EMAIL")
    password = os.environ.get("GARMIN_PASSWORD")

    if not email or not password:
        print("[ERROR] GARMIN_EMAIL / GARMIN_PASSWORD 환경변수 필요")
        sys.exit(1)

    client = Garmin(email, password)
    client.login()
    print("[INFO] Garmin login OK")
    return client

def upload_weight(client, date_str, time_str, weight):
    try:
        dt = datetime.strptime(f"{date_str} {time_str}", "%Y.%m.%d %H:%M:%S")
        dt = dt.replace(tzinfo=timezone.utc)

        epoch_ms = int(dt.timestamp() * 1000)

        data = {
            "date": dt.strftime("%Y-%m-%d"),
            "gmtTimestamp": epoch_ms,
            "value": float(weight),
            "unitKey": "kg"
        }

        resp = client.session.post(f"{BASE_URL}/{WEIGHT_POST_PATH}", json=data)
        resp.raise_for_status()
        print(f"[INFO] [OK] {date_str} {weight}kg @ {dt.isoformat()} ({epoch_ms})")
        return True
    except Exception as e:
        print(f"[INFO] [FAIL] {date_str} {weight}kg @ {dt.isoformat()} ({epoch_ms}) - {e}")
        return False

def main():
    df = load_csv_latest()
    client = login_garmin()

    success, fail = 0, 0
    for _, row in df.iterrows():
        date_str = str(row["날짜"]).split()[0]
        time_str = str(row["시간"])
        weight = row["몸무게"]

        if upload_weight(client, date_str, time_str, weight):
            success += 1
        else:
            fail += 1

    print(f"[INFO] Done. success={success}, failed={fail}")

if __name__ == "__main__":
    main()
