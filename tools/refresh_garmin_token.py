#!/usr/bin/env python3
"""
GARMIN_TOKEN 시크릿 재발급 도구.

GitHub Actions는 영구 디스크가 없어서 가민 토큰을 base64로 인코딩한 zip을
`GARMIN_TOKEN` 시크릿에 넣어두고 매 실행마다 복원한다. 그 시크릿 값을 만든다.

MFA 코드 입력이 필요하므로 반드시 로컬 PC에서 실행할 것.

    pip install garminconnect==0.3.2 garth
    python tools/refresh_garmin_token.py

생성된 garmin_token.b64 의 내용 전체를 GitHub의
Settings → Secrets and variables → Actions → GARMIN_TOKEN 에 붙여넣는다.
"""

import base64
import getpass
import os
import shutil
import sys
import tempfile
import zipfile
from pathlib import Path

try:
    from garminconnect import Garmin
except ImportError:
    sys.exit("❌ garminconnect 가 없습니다: pip install garminconnect==0.3.2 garth")

OUT_PATH = Path("garmin_token.b64")


def main() -> None:
    email = os.getenv("GARMIN_EMAIL") or input("Garmin 이메일: ").strip()
    password = os.getenv("GARMIN_PASSWORD") or getpass.getpass("Garmin 비밀번호: ")

    token_dir = Path(tempfile.mkdtemp(prefix="garmin-token-"))
    zip_path = token_dir.parent / "garmin_token.zip"

    try:
        api = Garmin(
            email=email,
            password=password,
            prompt_mfa=lambda: input("MFA 코드 입력: ").strip(),
        )
        api.login(str(token_dir))
        print(f"✅ 로그인 성공: {api.get_full_name()}")

        # 워크플로우가 `unzip -d ~/.garminconnect` 로 풀기 때문에
        # 토큰 파일들이 zip 최상단에 있어야 한다.
        token_files = sorted(f for f in token_dir.iterdir() if f.is_file())
        if not token_files:
            sys.exit(f"❌ 토큰 파일이 생성되지 않았습니다: {token_dir}")

        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for f in token_files:
                zf.write(f, f.name)
                print(f"   포함: {f.name}")

        OUT_PATH.write_text(base64.b64encode(zip_path.read_bytes()).decode())
        print(f"\n📄 생성 완료: {OUT_PATH.resolve()}")
        print("   이 파일의 내용 '전체'를 GitHub Secret GARMIN_TOKEN 에 붙여넣으세요.")
        print("   붙여넣은 뒤에는 이 파일을 삭제하세요 (로그인 자격증명과 동등합니다).")
    finally:
        shutil.rmtree(token_dir, ignore_errors=True)
        zip_path.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
