name: Update Garmin Weight

on:
  workflow_dispatch:
  schedule:
    - cron: '0 2 * * *'   # 매일 오전 11시 (KST 20시)

jobs:
  build:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout repo
        uses: actions/checkout@v3

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'

      - name: Install deps
        run: |
          python -m pip install --upgrade pip
          pip install -r requirements.txt
          # 👉 로그인 잘 되던 최신 조합으로 설치 (버전 핀 X)
          pip install --upgrade garminconnect garth

          # 확인용 버전 출력
          python - <<'PY'
          from importlib.metadata import version, PackageNotFoundError
          for p in ["garminconnect","garth","pandas","python-dateutil"]:
              try:
                  print(p, version(p))
              except PackageNotFoundError:
                  print(p, "not installed")
          PY

      - name: Restore Garmin token cache (~/.garminconnect)
        uses: actions/cache@v4
        with:
          path: ~/.garminconnect
          key: garmin-token-v1

      - name: Download latest CSV from Drive folder
        run: |
          rclone lsl gdrive: || true
          rclone copy gdrive: ./ --include "*.csv" --max-age 7d
          ls -l *.csv || true

      - name: Upload to Garmin
        env:
          GARMIN_EMAIL: ${{ secrets.GARMIN_EMAIL }}
          GARMIN_PASSWORD: ${{ secrets.GARMIN_PASSWORD }}
        run: |
          python garmin_weight_uploader.py

      - name: Save Garmin token cache (first run only)
        if: always()
        uses: actions/cache@v4
        with:
          path: ~/.garminconnect
          key: garmin-token-v1
