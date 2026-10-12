"""pytest 공통 픽스처 — plugins/ 를 import 경로에 추가하고 템플릿 설정을 제공한다."""

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "plugins"))

TEMPLATE = ROOT / "secrets" / "asset_flow.secrets.example.yaml"
APP_CONFIG = ROOT / "dags" / "config" / "asset_flow.yaml"


@pytest.fixture
def template_text() -> str:
    """
    Purpose: secrets 템플릿 원문
    Parameter: 없음
    Input: secrets/asset_flow.secrets.example.yaml
    Output: YAML 텍스트
    """
    return TEMPLATE.read_text(encoding="utf-8")


@pytest.fixture
def cfg():
    """
    Purpose: 템플릿 secrets 로드 결과
    Parameter: 없음
    Input: secrets 템플릿
    Output: SecretsConfig (마스킹 미등록)
    """
    from asset_flow.secrets_config import load_secrets

    return load_secrets(TEMPLATE, mask=False)


@pytest.fixture
def app():
    """
    Purpose: 저장소 config 로드 결과
    Parameter: 없음
    Input: dags/config/asset_flow.yaml
    Output: AppConfig
    """
    from asset_flow.app_config import load_app_config

    return load_app_config(APP_CONFIG)


@pytest.fixture
def write_yaml(tmp_path):
    """
    Purpose: 임시 YAML 파일 작성 함수 제공
    Parameter:
        - tmp_path: pytest 임시 디렉터리
    Input: 없음
    Output: _write(text, name) 함수
    """

    def _write(text: str, name: str = "s.yaml") -> Path:
        """
        Purpose: 텍스트를 임시 YAML 로 저장
        Parameter:
            - text: YAML 원문
            - name: 파일명 (기본 s.yaml)
        Input: 없음
        Output: 파일 경로
        """
        p = tmp_path / name
        p.write_text(text, encoding="utf-8")
        return p

    return _write
