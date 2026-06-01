import os
import json
import uuid
import jwt
import requests
from datetime import datetime
from pytz import timezone
from airflow.models import Variable
from ..config.kis import KIS


class TokenManager:
    def __init__(self):
        # 1. 한국 시간(KST) 기준 날짜 설정 (새벽 실행 시 UTC 문제 방지)
        kst = timezone("Asia/Seoul")
        self.CURRENT_DATE = datetime.now(kst).strftime("%Y%m%d")

        # 2. 파일 경로 설정 (data/tokens/20240101_token.json)
        self.DATA_DIR = os.getenv("TOKEN_DIR", "data/tokens")
        self.TOKEN_FILE_NAME = f"{self.CURRENT_DATE}_token.json"
        self.TOKEN_FILE_PATH = os.path.join(self.DATA_DIR, self.TOKEN_FILE_NAME)

    def _ensure_dir(self):
        """오늘 날짜 폴더 생성 및 과거 폴더 정리"""
        if not os.path.exists(self.DATA_DIR):
            os.makedirs(self.DATA_DIR, exist_ok=True)

    def _clean_old_tokens(self):
        """
        현재 날짜 파일이 아닌 과거 *_token.json 파일 삭제
        """
        if not os.path.exists(self.DATA_DIR):
            return
        for filename in os.listdir(self.DATA_DIR):
            if filename.endswith("_token.json") and filename != self.TOKEN_FILE_NAME:
                file_path = os.path.join(self.DATA_DIR, filename)
                try:
                    os.remove(file_path)
                except Exception as e:
                    raise Exception(f"[{filename}] 토큰 삭제 실패: {e}")

    def _get_kis_token(self, variable_name: str):
        """KIS 토큰 발급"""
        info = json.loads(Variable.get(variable_name))

        body = {
            "grant_type": "client_credentials",
            "appkey": info["appkey"],
            "appsecret": info["secret"],
        }

        try:
            response = requests.post(
                url=f"{KIS.BASE_URL}{KIS.PATHS['token']}",
                headers={"content-type": "application/json"},
                data=json.dumps(body),
                timeout=5,
            )
            response.raise_for_status()
            return response.json().get("access_token")
        except requests.exceptions.HTTPError as e:
            # 4xx/5xx 응답일 때
            print(
                f"[ERROR] {variable_name} 토큰 발급 실패 (HTTP {response.status_code}): {e}"
            )
            print("Response body:", response.text)  # 서버가 보낸 에러 내용
            raise

        except requests.exceptions.RequestException as e:
            # 네트워크/타임아웃 등 모든 requests 관련 예외
            print(f"[ERROR] {variable_name} 토큰 발급 실패 (네트워크 문제): {e}")
            raise

        except Exception as e:
            # 그 밖의 예외 (예: JSON 파싱 등)
            print(f"[ERROR] {variable_name} 토큰 발급 중 알 수 없는 오류: {e}")
            raise

    def _get_upbit_token(self, variable_name: str):
        """업비트 JWT 토큰 생성"""
        info = json.loads(Variable.get(variable_name))

        try:
            payload = {"access_key": info["appkey"], "nonce": str(uuid.uuid4())}
            token = jwt.encode(payload, info["secret"])
            return token
        except Exception as e:
            raise Exception(f"UPBIT 토큰 발급 실패: {e}")

    def TokenGenerator(self):
        """토큰 발급 및 저장"""
        if os.path.exists(self.TOKEN_FILE_PATH):
            return
        # 1. 토큰 폴더 확인 및 청소
        self._ensure_dir()
        self._clean_old_tokens()

        # 2. 토큰 발급
        try:
            tokens = {
                # "KIS_GOLD": self._get_kis_token("KIS_GOLD"),
                "KIS_STOCK": self._get_kis_token("KIS_STOCK"),
                "KIS_ISA": self._get_kis_token("KIS_ISA"),
                "KIS_PENSION": self._get_kis_token("KIS_PENSION"),
                "KIS_IRP": self._get_kis_token("KIS_IRP"),
                "UPBIT": self._get_upbit_token("UPBIT"),
            }
        except Exception as e:
            raise Exception(f"[ERROR] 토큰 발급 실패: {e}")

        # 3. 토큰 파일 저장
        with open(self.TOKEN_FILE_PATH, "w", encoding="utf-8") as f:
            json.dump(tokens, f, indent=4, ensure_ascii=False)

        return print(f"{self.TOKEN_FILE_NAME} 토큰 파일이 저장되었습니다.")

    def GetTokens(self):
        """토큰 조회 (없으면 생성)"""
        if not os.path.exists(self.TOKEN_FILE_PATH):
            self.TokenGenerator()

        with open(self.TOKEN_FILE_PATH, "r", encoding="utf-8") as f:
            tokens = json.load(f)

        return tokens
