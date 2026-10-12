import pandas as pd
import pytest

from asset_flow.config.schemas import BALANCE_COLUMNS
from asset_flow.pipeline import dispatch as dp
from asset_flow.secrets_config import load_secrets

DATES = {"standard_date": "2026-10-11", "base_date": "2026-10-12"}

DOMESTIC = {"output1": [{"pdno": "005930", "prdt_name": "삼성전자", "evlu_pfls_amt": "1,000", "evlu_pfls_rt": "10",
                         "pchs_avg_pric": "50000", "hldg_qty": "2", "pchs_amt": "100000", "evlu_amt": "101000",
                         "prpr": "50500"}]}
OVERSEAS = {"output1": [{"ovrs_pdno": "AAPL", "ovrs_item_name": "애플", "frcr_evlu_pfls_amt": "5", "evlu_pfls_rt": "2",
                         "pchs_avg_pric": "200", "ovrs_cblc_qty": "1", "frcr_pchs_amt1": "200",
                         "ovrs_stck_evlu_amt": "205", "now_pric2": "205", "tr_crcy_cd": "USD", "ovrs_excg_cd": "NASD"}]}


def _balance(fund_eval="1500000", cma_eval="300000"):
    """
    Purpose: KIS 계좌자산현황 응답 픽스처
    Parameter:
        - fund_eval: 펀드 평가금액
        - cma_eval: CMA 평가금액
    Input: 없음
    Output: 원시 응답 dict
    """
    rows = [{"pchs_amt": "0", "evlu_amt": "0", "evlu_pfls_amt": "0"} for _ in range(20)]
    rows[1] = {"pchs_amt": "1400000", "evlu_amt": fund_eval, "evlu_pfls_amt": "100000"}
    rows[14] = {"pchs_amt": "0", "evlu_amt": cma_eval, "evlu_pfls_amt": "0"}
    rows[7] = {"pchs_amt": "500000", "evlu_amt": "500858", "evlu_pfls_amt": "858"}       # RP/발행어음
    rows[19] = {"pchs_amt": "0", "evlu_amt": str(sum(float(r["evlu_amt"]) for r in rows[:19])),
                "evlu_pfls_amt": "0"}                                                   # 합계 행
    return {"rt_cd": "0", "output1": rows}


class FakeKIS:
    def __init__(self, token, config):
        """
        Purpose: 가짜 클라이언트 생성
        Parameter:
            - token: 토큰
            - config: 계좌 config
        Input: 없음
        Output: 없음
        """
        self.config = config

    def get_domestic_balance(self):
        """
        Purpose: 국내 잔고 응답
        Parameter: 없음
        Input: 없음
        Output: dict
        """
        return DOMESTIC

    def get_overseas_balance(self):
        """
        Purpose: 해외 잔고 응답
        Parameter: 없음
        Input: 없음
        Output: dict
        """
        return OVERSEAS

    def get_account_balance(self):
        """
        Purpose: 계좌자산현황 응답
        Parameter: 없음
        Input: 없음
        Output: dict
        """
        return _balance()


class FakeUpbit:
    def __init__(self, access_key, secret_key):
        """
        Purpose: 가짜 클라이언트 생성
        Parameter:
            - access_key: Upbit access key
            - secret_key: Upbit secret key
        Input: 없음
        Output: 없음
        """
        pass

    def get_balance(self):
        """
        Purpose: Upbit 보유 자산 응답
        Parameter: 없음
        Input: 없음
        Output: list
        """
        return [{"currency": "KRW", "balance": "10", "avg_buy_price": "0"},
                {"currency": "BTC", "balance": "0.5", "avg_buy_price": "100000000"}]

    def get_market_codes(self):
        """
        Purpose: Upbit 마켓 목록 응답
        Parameter: 없음
        Input: 없음
        Output: list
        """
        return [{"market": "KRW-BTC", "korean_name": "비트코인"}, {"market": "BTC-ETH", "korean_name": "이더리움"}]

    def get_daily_candles(self, markets, to_date):
        """
        Purpose: Upbit 일 캔들 응답
        Parameter:
            - markets: 마켓 목록
            - to_date: 캔들 경계일
        Input: 없음
        Output: list
        """
        assert to_date == "2026-10-12"
        return [{"market": "KRW-BTC", "trade_price": 120000000}]


class FakeTokens:
    def token_for(self, cred_id):
        """
        Purpose: 가짜 토큰 반환
        Parameter:
            - cred_id: credential id
        Input: 없음
        Output: 토큰 문자열
        """
        return f"T-{cred_id}"


@pytest.fixture
def two_irp_cfg(template_text, write_yaml):
    """
    Purpose: IRP 계좌 2개를 가진 secrets 설정
    Parameter:
        - template_text: secrets 템플릿 원문
        - write_yaml: 임시 YAML 작성 함수
    Input: 없음
    Output: SecretsConfig
    """
    extra = ('  - id: IRP2\n    kind: manual_fund\n    account_type: IRP\n    account_no: "00000099-29"\n'
             '    display_name: 저축IRP2\n\n  - id: DC\n')
    return load_secrets(write_yaml(template_text.replace("  - id: DC\n", extra, 1)), mask=False)


@pytest.fixture
def deps(monkeypatch, two_irp_cfg, app):
    """
    Purpose: API·DB 를 가짜로 바꾼 Deps
    Parameter:
        - monkeypatch: pytest monkeypatch
        - two_irp_cfg: IRP 2개 secrets 설정
        - app: 비밀 아닌 설정
    Input: 없음
    Output: Deps
    """
    monkeypatch.setattr(dp, "KISApiClient", FakeKIS)
    monkeypatch.setattr(dp, "UpbitApiClient", FakeUpbit)
    monkeypatch.setattr(dp.db, "get_fund_price", lambda engine, code, d: (1000.0, f"펀드-{code}"))
    ledger = pd.DataFrame([
        {"standard_date": "2026-10-01", "account_code": two_irp_cfg.account(i).account_code,
         "product_code": "K55105D43299", "holding_quantity": q, "total_purchase_amount": 1_000_000.0,
         "account_name": n, "account_type": t}
        for i, q, n, t in (("IRP", 1_200_000.0, "저축IRP", "IRP"), ("IRP2", 900_000.0, "저축IRP2", "IRP"),
                           ("DC", 1_100_000.0, "확정기여형DC", "DC"))
    ])
    monkeypatch.setattr(dp.db, "get_manual_positions", lambda engine, d: ledger)
    monkeypatch.setattr(dp.db, "ensure_writable", lambda engine: None)   # 가드는 test_db_guard 에서 검증
    return dp.Deps(two_irp_cfg, app, engine=None, tokens=FakeTokens())


def _run(kind_id, deps):
    """
    Purpose: kind 핸들러 실행 + 컬럼 순서 검증
    Parameter:
        - kind_id: 계좌 id
        - deps: 의존성
    Input: 없음
    Output: DataFrame
    """
    acct = deps.secrets.account(kind_id)
    df = dp.HANDLERS[acct.kind](acct, DATES, deps)
    assert list(df.columns) == BALANCE_COLUMNS
    return df


def test_kis_stock_overseas_plus_domestic(deps):
    df = _run("KIS_STOCK", deps)
    assert list(df["product_code"]) == ["AAPL", "005930", "RP_NOTE"]
    assert set(df["account_code"]) == {"00000000-01"} and set(df["account_name"]) == {"위탁"}
    assert df.loc[1, "total_profit_amount"] == 1000.0                   # 쉼표 정규화


def test_pension_fund_back_calculates_units(deps):
    df = _run("KIS_PENSION_DEDUCTIBLE", deps)
    fund = df[df["asset_type"] == "FUND"].iloc[0]
    assert fund["product_code"] == "K553W5E17401" and fund["product_name"] == "펀드-K553W5E17401"
    assert fund["holding_quantity"] == 1_500_000 / 1000.0 / 0.001
    assert len(df) == 2


def test_cma_and_upbit(deps):
    cma = _run("KIS_CMA", deps).iloc[0]
    assert (cma["product_code"], cma["account_code"]) == ("CMA", "00000004-00")
    assert cma["total_evaluation_amount"] == 300000.0
    up = _run("UPBIT", deps)
    assert list(up["product_code"]) == ["BTC"] and up.iloc[0]["account_code"] == "UPBIT"
    assert up.iloc[0]["total_evaluation_amount"] == 0.5 * 120000000 and up.iloc[0]["account_name"] == "가상자산"


def test_manual_same_type_accounts_pick_own_row(deps):
    irp, irp2 = _run("IRP", deps).iloc[0], _run("IRP2", deps).iloc[0]
    assert irp["account_code"] == "00000005-29" and irp2["account_code"] == "00000099-29"
    assert irp["total_evaluation_amount"] == pytest.approx(1_200_000 * 1000.0 * 0.001)
    assert irp2["total_evaluation_amount"] == pytest.approx(900_000 * 1000.0 * 0.001)


def test_manual_fund_nav_missing_raises(deps, monkeypatch):
    monkeypatch.setattr(dp.db, "get_fund_price", lambda *a: (None, None))
    with pytest.raises(ValueError, match="NAV 결측"):
        _run("DC", deps)


def test_collect_account_empty_raises_before_write(deps, monkeypatch):
    writes = []
    monkeypatch.setattr(dp.db, "delete_and_insert_account_assets", lambda *a: writes.append(a) or 0)
    with pytest.raises(RuntimeError, match=r"\[INSTALLMENT_SAVINGS\] 데이터 없음"):
        dp.collect_account("INSTALLMENT_SAVINGS", DATES, deps)       # 원장 행 없음
    assert writes == []


def test_collect_account_writes_scoped(deps, monkeypatch):
    writes = []
    def fake_write(engine, schema, table, d, code, records):
        """
        Purpose: 적재 호출 캡처
        Parameter:
            - engine: DB 엔진
            - schema: 스키마
            - table: 테이블
            - d: 기준일
            - code: 펀드코드
            - records: 적재 레코드
        Input: 없음
        Output: 적재 행 수
        """
        writes.append((schema, table, d, code))
        return len(records)

    monkeypatch.setattr(dp.db, "delete_and_insert_account_assets", fake_write)
    assert dp.collect_account("KIS_ISA", DATES, deps) == 1
    assert writes == [("account", "asset_daily", "2026-10-11", "00000001-00")]


def test_fund_codes_to_crawl(cfg, app, template_text, write_yaml):
    assert dp.fund_codes_to_crawl(cfg, app, ["KX0000000001"]) == ["K55105D43299", "K553W5E17401", "KX0000000001"]
    text = template_text.replace('    # fund_code: "K553W5E17401"', '    fund_code: "KR5000000002"')
    override = load_secrets(write_yaml(text), mask=False)
    assert "KR5000000002" in dp.fund_codes_to_crawl(override, app)
    assert dp.manual_product(cfg.account("DC"), app) == ("K55105D43299", 0.001)
    assert dp.manual_product(cfg.account("INSTALLMENT_SAVINGS"), app) == (None, 1.0)


def test_exchange_rate_cross_conversion():
    from asset_flow.transformers.kis_transformer import transform_exchange_rate

    def raw(name, rate):
        """
        Purpose: KIS 환율 응답 픽스처
        Parameter:
            - name: 이름
            - rate: 환율
        Input: 없음
        Output: 원시 응답 dict
        """
        return {"output1": [{"hts_kor_isnm": name, "stck_shrn_iscd": name, "ovrs_nmix_prpr": rate,
                             "ovrs_nmix_prdy_clpr": rate, "ovrs_prod_oprc": rate, "ovrs_prod_hgpr": rate,
                             "ovrs_prod_lwpr": rate}]}

    df = transform_exchange_rate([raw("원/달러", "1400"), raw("원/엔", "9.5"), raw("달러/파운드", "1.25"),
                                  raw("달러/유로", "1.1")], "2026-10-11")
    rates = dict(zip(df["currency_code"], df["base_rate"], strict=True))
    assert rates == {"USD": 1400.0, "JPY": 9.5, "GBP": 1750.0, "EUR": pytest.approx(1540.0)}
    assert set(df["currency_pair_name"]) == {"원/달러", "원/엔", "원/파운드", "원/유로"}


def test_collect_account_guard_blocks_before_api(deps, monkeypatch):
    def deny(engine):
        """
        Purpose: 쓰기 가드 실패 흉내
        Parameter:
            - engine: DB 엔진
        Input: 없음
        Output: 없음 (RuntimeError)
        """
        raise RuntimeError("쓰기 차단")

    monkeypatch.setattr(dp.db, "ensure_writable", deny)
    monkeypatch.setattr(dp, "HANDLERS", {})                      # 핸들러(=API)까지 가면 KeyError
    with pytest.raises(RuntimeError, match="쓰기 차단"):
        dp.collect_account("KIS_ISA", DATES, deps)


def test_collect_account_rejects_disabled(deps, two_irp_cfg):
    # frozen 모델 → 해당 계좌만 enabled=False 인 복사본으로 교체
    disabled = dp.Deps(two_irp_cfg.model_copy(update={"accounts": [
        a.model_copy(update={"enabled": False}) if a.id == "KIS_ISA" else a for a in two_irp_cfg.accounts]}),
        deps.app, engine=None, tokens=deps.tokens)
    with pytest.raises(RuntimeError, match="enabled=false"):
        dp.collect_account("KIS_ISA", DATES, disabled)
