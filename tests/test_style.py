"""코드 규칙 테스트 — docstring 형식(Purpose/Parameter/Input/Output), 파싱 시점 secrets 금지."""

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SECTIONS = ("Purpose:", "Parameter:", "Input:", "Output:")


def _sources() -> list[Path]:
    """
    Purpose: 규칙 검사 대상 신규 소스 목록
    Parameter: 없음
    Input: plugins/, dags/, tests/conftest.py
    Output: 파일 경로 리스트
    """
    files = list((ROOT / "plugins").rglob("*.py")) + list((ROOT / "dags").glob("*.py"))
    return files + [ROOT / "tests" / "conftest.py"]


def test_every_function_has_spec_docstring():
    bad = []
    for path in _sources():
        tree = ast.parse(path.read_text(encoding="utf-8"))
        if not ast.get_docstring(tree):
            bad.append(f"{path.name}: 모듈 docstring 없음")
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                missing = [s for s in SECTIONS if s not in (ast.get_docstring(node) or "")]
                if missing:
                    bad.append(f"{path.relative_to(ROOT)}:{node.lineno} {node.name} 누락 {missing}")
    assert bad == []


def test_dags_do_not_import_loaders_at_top_level():
    for path in (ROOT / "dags").glob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        names = {a.name for n in tree.body if isinstance(n, (ast.Import, ast.ImportFrom)) for a in n.names}
        assert not names & {"load_secrets", "load_app_config", "Variable"}, path.name


def test_test_helpers_have_spec_docstring():
    # 테스트 함수(test_*)는 제외, 보조 함수·픽스처·가짜 클래스 메서드는 규칙 적용
    bad = []
    for path in (ROOT / "tests").glob("*.py"):
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, ast.FunctionDef) and not node.name.startswith("test_"):
                if any(s not in (ast.get_docstring(node) or "") for s in SECTIONS):
                    bad.append(f"{path.name}:{node.lineno} {node.name}")
    assert bad == []
