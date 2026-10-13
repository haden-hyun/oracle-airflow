"""
한국투자증권(KIS) OpenAPI 클라이언트

- 토큰은 호출부가 주입 (token_for 또는 existing_token)
- config 는 secrets_config.kis_client_config 로 만든 dict
- 제공: get_overseas_balance / get_domestic_balance / get_account_balance / get_exchange_rate (원시 JSON 반환)
- 공통: _request(단건, rt_cd 검사·간격 유지) / _get_pages(tr_cont 연속조회) — 배당·시세 전용 (잔고·환율은 safe_request 직접)
- 배당: get_ksd_dividend / get_period_rights / get_overseas_period_rights / get_overseas_rights_ice (원시 행)
- 시세: get_domestic_daily_price / get_overseas_daily_price (원주가, 100행씩 날짜를 당겨 반복)
- 계좌번호가 응답 본문(ctx_area_*·acno10)에 평문으로 올 수 있음 → _redact 로 실패 로그·예외에서 가림
"""

from __future__ import annotations

import datetime as dt
import re
import time

from asset_flow.clients.base_client import BaseApiClient
from asset_flow.config.kis import KIS

# 응답 본문의 계좌번호 필드 (ctx_area_nk100 · ctx_area_fk100 · acno10 · cano) 값
_ACCOUNT_FIELD = re.compile(r'("(?:ctx_area_[a-z0-9]+|acno\d*|cano)"\s*:\s*")[^"]*(")', re.IGNORECASE)


class KisApiError(RuntimeError):
    """KIS 응답 rt_cd != "0" (HTTP 200 오류). 메시지에 tr_id·msg_cd·msg1 만 — 파라미터·ctx 값 없음."""

    def __init__(self, tr_id: str, msg_cd: str | None, msg1: str | None):
        """
        Purpose: 오류 정보 보관
        Parameter:
            - tr_id: 거래 ID
            - msg_cd: KIS 메시지 코드 (예: OPSQ2000)
            - msg1: KIS 메시지 (뒤 공백 제거)
        Input: 없음
        Output: 없음
        """
        self.tr_id, self.msg_cd, self.msg1 = tr_id, msg_cd, (msg1 or "").strip()
        super().__init__(f"KIS {tr_id} 실패 (msg_cd={msg_cd}): {self.msg1}")


def _prev_day(yyyymmdd: str) -> str:
    """
    Purpose: YYYYMMDD 하루 전
    Parameter:
        - yyyymmdd: 날짜 문자열
    Input: 없음
    Output: 하루 전 YYYYMMDD
    """
    return (dt.datetime.strptime(yyyymmdd, "%Y%m%d") - dt.timedelta(days=1)).strftime("%Y%m%d")


class KISApiClient(BaseApiClient):
    """한국투자증권 API 클라이언트."""

    def __init__(self, token: str, config: dict):
        """
        Purpose: 토큰·계좌 설정 보관
        Parameter:
            - token: KIS 액세스 토큰
            - config: {appkey, secret[, account, product_code, type]}
        Input: 없음
        Output: 없음
        """
        super().__init__(base_url=KIS.BASE_URL)
        self.token = token
        self.config = config
        self._last_call = 0.0                    # 마지막 _request 시각 (REQUEST_INTERVAL 유지)

    def _build_headers(self, tr_id: str, tr_cont: str | None = None) -> dict[str, str]:
        """
        Purpose: KIS 공통 인증 헤더 생성
        Parameter:
            - tr_id: 거래 ID
            - tr_cont: 연속조회 ("" 첫 요청, "N" 다음 페이지). None 이면 tr_cont·custtype 생략 (잔고·환율)
        Input: 없음
        Output: 헤더 dict (authorization, appKey, appSecret, tr_id [, tr_cont, custtype])
        """
        headers = {
            "content-type": "application/json",
            "authorization": f"Bearer {self.token}",
            "appKey": self.config["appkey"],
            "appSecret": self.config["secret"],
            "tr_id": tr_id,
        }
        if tr_cont is not None:
            headers |= {"tr_cont": tr_cont, "custtype": KIS.CUSTTYPE}
        return headers

    def _request(self, name: str, params: dict, tr_cont: str = "") -> tuple[dict, str]:
        """
        Purpose: KIS GET 1회 (요청 간격 유지 + rt_cd 검사) — 배당·시세 조회의 단일 진입점
        Parameter:
            - name: KIS.PATHS/TR_IDS 키
            - params: 쿼리 파라미터
            - tr_cont: 요청 헤더 tr_cont ("" 또는 "N")
        Input: KIS API
        Output: (응답 JSON, 응답 헤더 tr_cont 공백 제거값). rt_cd != "0" 이면 KisApiError
                ("조회할 자료가 없습니다" 는 rt_cd=0 → 정상 반환)
        """
        wait = KIS.REQUEST_INTERVAL - (time.monotonic() - self._last_call)
        if wait > 0:
            time.sleep(wait)
        url = self._build_url(KIS.PATHS[name])
        try:
            resp = self.safe_request("GET", url, headers=self._build_headers(KIS.TR_IDS[name], tr_cont), params=params)
        finally:
            self._last_call = time.monotonic()
        body = resp.json()
        if body.get("rt_cd") != "0":
            raise KisApiError(KIS.TR_IDS[name], body.get("msg_cd"), body.get("msg1"))
        return body, (resp.headers.get("tr_cont") or "").strip()

    def _get_pages(self, name: str, params: dict, list_key: str, ctx_keys: tuple[str, str] | None = None,
                   max_pages: int = KIS.MAX_PAGES) -> list[dict]:
        """
        Purpose: tr_cont 연속조회 공통 처리 — 응답 헤더 M/F 이고 ctx 값이 있으면 다음 페이지
        Parameter:
            - name: KIS.PATHS/TR_IDS 키
            - params: 첫 요청 파라미터 (ctx 키는 "" 로 포함)
            - list_key: 응답 행 목록 키 ("output" / "output1")
            - ctx_keys: (NK 키, FK 키) 응답 본문 소문자 이름. None 이면 1페이지
            - max_pages: 최대 페이지 (기본 KIS.MAX_PAGES)
        Input: KIS API
        Output: 전 페이지 행 리스트 (0건이면 []). 상한 초과 RuntimeError, rt_cd 오류 KisApiError.
                ctx 값은 계좌번호 포함 → 반환·로그하지 않음
        """
        params, rows, cont = dict(params), [], ""
        for _ in range(max_pages):
            body, next_cont = self._request(name, params, cont)
            rows += body.get(list_key) or []
            if ctx_keys is None or next_cont not in KIS.CONTINUE_CODES:
                return rows
            ctx = {k.upper(): body.get(k) or "" for k in ctx_keys}
            if not any(v.strip() for v in ctx.values()):
                return rows
            params |= ctx                          # 공백 패딩 그대로 되돌려 보냄
            cont = "N"
        raise RuntimeError(f"KIS {KIS.TR_IDS[name]} 연속조회 {max_pages}페이지 초과")

    def _redact(self, text: str) -> str:
        """
        Purpose: 실패 로그·예외용 본문에서 계좌번호 가리기
        Parameter:
            - text: 응답 본문
        Input: 없음
        Output: ctx_area_*·acno*·cano 값과 이 클라이언트 계좌번호를 ******** 로 바꾼 문자열
        """
        text = _ACCOUNT_FIELD.sub(r"\1********\2", text)
        account = self.config.get("account")
        return text.replace(account, "********") if account else text

    def _get_account(self, name: str) -> dict:
        """
        Purpose: 계좌 단위 조회 공통 처리 (CANO·상품코드 + 고정 파라미터)
        Parameter:
            - name: KIS.PATHS/TR_IDS/PARAMS 키
        Input: KIS API
        Output: 원시 JSON
        """
        params = {"CANO": self.config["account"], "ACNT_PRDT_CD": self.config["product_code"], **KIS.PARAMS[name]}
        url = self._build_url(KIS.PATHS[name])
        return self.safe_request("GET", url, headers=self._build_headers(KIS.TR_IDS[name]), params=params).json()

    def get_overseas_balance(self) -> dict:
        """
        Purpose: 해외주식 잔고 조회 (TTTS3012R)
        Parameter: 없음
        Input: KIS API
        Output: 원시 JSON (output1 = 종목별 잔고)
        """
        return self._get_account("overseas_balance")

    def get_domestic_balance(self) -> dict:
        """
        Purpose: 국내주식 잔고 조회 (TTTC8434R)
        Parameter: 없음
        Input: KIS API
        Output: 원시 JSON (output1 = 종목별 잔고)
        """
        return self._get_account("domestic_balance")

    def get_account_balance(self) -> dict:
        """
        Purpose: 투자계좌자산현황 조회 (CTRP6548R, 전일 기준가 적용) — 연금저축·CMA 용
        Parameter: 없음
        Input: KIS API
        Output: 원시 JSON (output1[1]=펀드/MMW, output1[14]=외화단기사채(CMA)).
                rt_cd != "0" 이면 ValueError — 미보유로 오인해 기존 적재분을 지우지 않도록
        """
        data = self._get_account("account_balance")
        if data.get("rt_cd") != "0":
            raise ValueError(f"KIS 투자계좌자산현황 조회 실패 (rt_cd={data.get('rt_cd')}): {data.get('msg1')}")
        return data

    def get_exchange_rate(self, standard_date: str, product_codes: list[str]) -> list[dict]:
        """
        Purpose: 환율 기간별 시세 조회 (FHKST03030100, 계좌번호 불필요)
        Parameter:
            - standard_date: 기준일 (YYYY-MM-DD)
            - product_codes: 통화 상품코드 리스트
        Input: KIS API
        Output: 통화별 원시 JSON 리스트
        """
        url = self._build_url(KIS.PATHS["exchange_rate"])
        headers = self._build_headers(KIS.TR_IDS["exchange_rate"])
        day = standard_date.replace("-", "")
        results = []
        for code in product_codes:
            params = {"FID_INPUT_ISCD": code, "FID_INPUT_DATE_1": day, "FID_INPUT_DATE_2": day,
                      **KIS.PARAMS["exchange_rate"]}
            results.append(self.safe_request("GET", url, headers=headers, params=params).json())
        return results

    # ── 배당 ──
    def get_ksd_dividend(self, product_code: str, start: str, end: str) -> list[dict]:
        """
        Purpose: 국내 종목 배당·분배 일정 (HHKDB669102C0, 계좌 불필요, 종목 전 이력이 1페이지)
        Parameter:
            - product_code: 국내 6자리 코드 (0228G0 같은 영숫자 포함)
            - start: 기준일 범위 시작 YYYYMMDD
            - end: 기준일 범위 끝 YYYYMMDD
        Input: KIS API
        Output: output1 원시 행 리스트 (이력 없으면 [])
        """
        params = {**KIS.PARAMS["ksd_dividend"], "F_DT": start, "T_DT": end, "SHT_CD": product_code}
        return self._get_pages("ksd_dividend", params, "output1")

    def get_period_rights(self, start: str, end: str) -> list[dict]:
        """
        Purpose: 계좌 권리(분배금·배당) 원장 (CTRGA011R, 25행 연속조회, 계좌 자기 키 필수)
        Parameter:
            - start: 기준일(bass_dt) 범위 시작 YYYYMMDD
            - end: 기준일 범위 끝 YYYYMMDD
        Input: KIS API
        Output: output 원시 행 리스트 (KIER2620 '조회할 자료가 없습니다' → []).
                다른 계좌 키면 KisApiError(OPSQ2000). 원시 행엔 acno10(계좌번호) 포함 → 호출부 밖으로 내보내지 않음
        """
        params = {**KIS.PARAMS["period_rights"], "CANO": self.config["account"],
                  "ACNT_PRDT_CD": self.config["product_code"], "INQR_STRT_DT": start, "INQR_END_DT": end}
        return self._get_pages("period_rights", params, "output", ("ctx_area_nk100", "ctx_area_fk100"))

    def get_overseas_period_rights(self, symbol: str, prdt_type: str, start: str, end: str,
                                   right_type: str = "03") -> list[dict]:
        """
        Purpose: 해외 종목 주당배당 (CTRGT011R, 종목별 — PDNO 공백이면 전 종목이 섞여 옴)
        Parameter:
            - symbol: 티커 (asset_daily.product_code)
            - prdt_type: 512 나스닥 / 513 뉴욕 / 529 아멕스 (KIS.OVERSEAS_PRDT_TYPE)
            - start: 현지기준일 범위 시작 YYYYMMDD
            - end: 현지기준일 범위 끝 YYYYMMDD
            - right_type: 03 현금배당 / 75 특별배당 (기본 03)
        Input: KIS API
        Output: output 원시 행 리스트 (KIER2520 '조회된 자료가 없습니다' → [])
        """
        params = {**KIS.PARAMS["overseas_period_rights"], "RGHT_TYPE_CD": right_type, "INQR_STRT_DT": start,
                  "INQR_END_DT": end, "PDNO": symbol, "PRDT_TYPE_CD": prdt_type}
        return self._get_pages("overseas_period_rights", params, "output", ("ctx_area_nk50", "ctx_area_fk50"))

    def get_overseas_rights_ice(self, symbol: str, start: str, end: str) -> list[dict]:
        """
        Purpose: 해외 권리 일정 (HHDFS78330900, 공시일 기준 범위·약 5년 한도, 금액 없음, 연속조회 없음)
        Parameter:
            - symbol: 티커
            - start: 공시일 범위 시작 YYYYMMDD (기준일보다 60일 이상 앞)
            - end: 공시일 범위 끝 YYYYMMDD
        Input: KIS API
        Output: output1 원시 행 리스트 (공개매수·주식분할 등 비배당 포함, 각 행에 SYMB 추가)
        """
        params = {**KIS.PARAMS["overseas_rights_ice"], "SYMB": symbol, "ST_YMD": start, "ED_YMD": end}
        return [r | {"SYMB": symbol} for r in self._get_pages("overseas_rights_ice", params, "output1")]

    # ── 시세 ──
    def get_domestic_daily_price(self, code: str, start: str, end: str) -> list[dict]:
        """
        Purpose: 국내 종목 일봉 기간 조회 (FHKST03010100, 원주가) — 100행씩 끝 날짜를 당겨 반복
        Parameter:
            - code: 6자리 종목코드
            - start: 기간 시작 YYYYMMDD (포함)
            - end: 기간 끝 YYYYMMDD (포함)
        Input: KIS API
        Output: output2 원시 행 리스트 (최근→과거). 상장 전·잘못된 코드는 [] (API 가 에러를 주지 않음)
        """
        rows, cur = [], end
        while cur >= start:
            params = {**KIS.PARAMS["domestic_daily_price"], "FID_INPUT_ISCD": code,
                      "FID_INPUT_DATE_1": start, "FID_INPUT_DATE_2": cur}
            page = [r for r in self._request("domestic_daily_price", params)[0].get("output2") or []
                    if r.get("stck_bsop_date")]
            rows += page
            if len(page) < KIS.PRICE_PAGE_SIZE:
                break
            cur = _prev_day(min(r["stck_bsop_date"] for r in page))
        return rows

    def get_overseas_daily_price(self, excd: str, symbol: str, start: str, end: str) -> list[dict]:
        """
        Purpose: 해외 종목 일봉 기간 조회 (HHDFS76240000, 원주가) — BYMD 를 당겨 100행씩 반복
        Parameter:
            - excd: 시세 API 거래소 코드 (NAS/NYS/AMS — KIS.OVERSEAS_PRICE_EXCD 로 변환한 값)
            - symbol: 티커
            - start: 현지 일자 시작 YYYYMMDD (포함)
            - end: 현지 일자 끝 YYYYMMDD (포함)
        Input: KIS API
        Output: output2 원시 행 중 start~end 만 (최근→과거). 거래소가 틀리면 [] (API 가 에러를 주지 않음)
        """
        rows, cur = [], end
        while cur >= start:
            params = {**KIS.PARAMS["overseas_daily_price"], "EXCD": excd, "SYMB": symbol, "BYMD": cur}
            page = [r for r in self._request("overseas_daily_price", params)[0].get("output2") or []
                    if r.get("xymd")]
            rows += [r for r in page if start <= r["xymd"] <= end]
            if len(page) < KIS.PRICE_PAGE_SIZE:
                break
            cur = _prev_day(min(r["xymd"] for r in page))
        return rows
