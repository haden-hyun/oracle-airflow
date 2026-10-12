import pytest

from asset_flow import secrets_config as sc
from conftest import TEMPLATE

# 거부 사례 12종: (이름, 원문 → 변형, 오류 메시지에 기대하는 조각)
REJECTS = [
    ("dup-id", ("  - id: DC\n", "  - id: IRP\n"), "accounts.id 중복"),
    ("dup-field", ("    display_name: 저축IRP\n", "    display_name: 저축IRP\n    display_name: X\n"),
     "중복 키 'display_name'"),
    ("unquoted01", ('product_code: "01"', "product_code: 01"), "accounts.0.product_code"),
    ("unquoted-cano-leading0", ('cano: "00000000"', "cano: 00000000"), "accounts.0.cano"),
    ("bad-ref", ("credential: KIS_ISA            # ISA", "credential: KIS_IAS            # ISA"),
     "정의되지 않은 'KIS_IAS'"),
    ("missing-expiry", ("    expires_on: 2027-05-01\n", ""), "expires_on: Field required"),
    ("manual-with-cano", ("    account_type: IRP              # account_master.account_type\n",
                          "    account_type: IRP\n    cano: \"00000009\"\n"), "허용 안 됨 ['cano']"),
    ("yaml-syntax", ("version: 1\n", "version: 1\n  bad: [\n"), "YAML 문법 오류"),
    ("dup-account-no", ('account_no: "000-000-000006"', 'account_no: "00000005-29"'), "같은 계좌번호"),
    ("bad-date", ("issued_on: 2026-03-31", "issued_on: 2026-02-30"), "issued_on"),
    ("typo-field", ("issued_on: 2026-03-31", "issue_on: 2026-03-31"), "Extra inputs are not permitted"),
    ("cano-7", ('cano: "00000001"', 'cano: "0000001"'), "accounts.1.cano"),
]


def _secrets_in(text, cfg):
    """
    Purpose: 문자열에 더미 비밀값 포함 여부
    Parameter:
        - text: 검사할 문자열
        - cfg: secrets 설정
    Input: 없음
    Output: 포함된 값 리스트
    """
    return [v for v in sc.secret_values(cfg) if v in text]


def test_template_passes(cfg):
    assert len(cfg.accounts) == 10
    assert {c.id for c in cfg.credentials} >= {"KIS_STOCK", "UPBIT", "DATA_GO_KR"}


@pytest.mark.parametrize("name,change,expected", REJECTS, ids=[r[0] for r in REJECTS])
def test_rejects_without_leaking_values(name, change, expected, template_text, write_yaml, cfg):
    old, new = change
    assert old in template_text, f"{name}: 템플릿에서 변형 대상 문자열을 찾지 못함"
    with pytest.raises(sc.SecretsConfigError) as ei:
        sc.load_secrets(write_yaml(template_text.replace(old, new, 1)), mask=False)
    msg = str(ei.value)
    assert expected in msg
    assert _secrets_in(msg, cfg) == []
    assert ei.value.__cause__ is None and ei.value.__suppress_context__


def test_fund_code_only_for_fund_kinds(template_text, write_yaml):
    bad = template_text.replace("    display_name: CMA\n", '    display_name: CMA\n    fund_code: "K553W5E17401"\n')
    with pytest.raises(sc.SecretsConfigError, match="fund_code"):
        sc.load_secrets(write_yaml(bad), mask=False)
    ok = template_text.replace('    # fund_code: "K553W5E17401"', '    fund_code: "K553W5E17401"')
    assert sc.load_secrets(write_yaml(ok), mask=False).account("KIS_PENSION_NON_DEDUCTIBLE").fund_code == "K553W5E17401"


def test_app_secret_optional_only_for_data_go_kr(template_text, write_yaml):
    bad = template_text.replace('    app_secret: "<KIS_APPSECRET_ISA>"\n', "", 1)
    with pytest.raises(sc.SecretsConfigError, match="app_secret 필수"):
        sc.load_secrets(write_yaml(bad), mask=False)


def test_holiday_credential_must_be_data_go_kr(template_text, write_yaml):
    bad = template_text.replace("holiday_api_credential: DATA_GO_KR", "holiday_api_credential: KIS_STOCK")
    with pytest.raises(sc.SecretsConfigError, match="data_go_kr"):
        sc.load_secrets(write_yaml(bad), mask=False)


def test_missing_file_fails_closed(tmp_path):
    with pytest.raises(sc.SecretsConfigError, match="secrets 파일 없음"):
        sc.load_secrets(tmp_path / "none.yaml")


def test_mask_registers_all_secret_values(monkeypatch, cfg):
    seen = []
    monkeypatch.setattr(sc, "mask_secret", seen.append)
    sc.load_secrets(TEMPLATE, mask=True)
    expected = {v for c in cfg.credentials for v in (c.app_key, c.app_secret) if v}
    expected |= {a.cano for a in cfg.accounts if a.cano} | {a.account_no for a in cfg.accounts if a.account_no}
    assert set(seen) == expected
    assert "01" not in seen and "UPBIT" not in seen          # 상품코드·id 는 마스킹하지 않음


def test_credentials_in_use_and_used_by(template_text, write_yaml, cfg):
    assert cfg.used_by("KIS_ISA") == ("KIS_ISA", "KIS_CMA")
    assert cfg.used_by("KIS_STOCK") == ("KIS_STOCK", "exchange_rate")
    assert "DATA_GO_KR" in {c.id for c in cfg.credentials_in_use()}
    extra = template_text.replace(
        "credentials:\n",
        'credentials:\n  - id: KIS_IRP\n    provider: kis\n    app_key: "<KIS_APPKEY_IRP>"\n'
        '    app_secret: "<KIS_APPSECRET_IRP>"\n    expires_on: 2027-03-31\n', 1,
    ).replace("  holiday_api_credential: DATA_GO_KR", "")
    in_use = {c.id for c in sc.load_secrets(write_yaml(extra), mask=False).credentials_in_use()}
    assert "KIS_IRP" not in in_use and "DATA_GO_KR" not in in_use


def test_account_code_derivation(cfg):
    assert cfg.account("KIS_STOCK").account_code == "00000000-01"
    assert cfg.account("DC").account_code == "000-000-000006"
    assert cfg.account("UPBIT").account_code == "UPBIT"


def test_fingerprint(cfg):
    cred = cfg.credential("KIS_STOCK")
    fp = sc.credential_fingerprint(cred)
    assert len(fp) == 16 and fp == sc.credential_fingerprint(cred)
    assert sc.credential_fingerprint(cred.model_copy(update={"app_secret": "<OTHER_SECRET>"})) != fp
    assert sc.credential_fingerprint(cred.model_copy(update={"app_key": "<OTHER_KEY__>"})) != fp


def test_kis_client_config_shape(cfg):
    conf = sc.kis_client_config(cfg.credential("KIS_ISA"), cfg.account("KIS_CMA"))
    assert conf == {"appkey": "<KIS_APPKEY_ISA>", "secret": "<KIS_APPSECRET_ISA>",
                    "account": "00000004", "product_code": "00", "type": "CMA"}


def test_cli_exit_codes(template_text, write_yaml, capsys):
    far = template_text.replace("expires_on: 2027-", "expires_on: 2099-")
    far = far.replace("expires_on: 2028-", "expires_on: 2099-")
    cur = write_yaml(far)
    assert sc.main([str(cur)]) == 0
    assert sc.main([str(write_yaml("version: [", "bad.yaml"))]) == 1
    prev = write_yaml(far.replace('cano: "00000001"', 'cano: "00000011"'), "prev.yaml")
    assert sc.main([str(cur), "--previous", str(prev)]) == 2
    out = capsys.readouterr().out
    assert "KIS_ISA: account_code 변경" in out and "00000011" not in out
