"""
fundguide.net 펀드 기준가 크롤러 (Playwright headless Chromium)

- crawl_fund_prices(codes, standard_date): 코드별 기준가 → fund_price_daily 레코드 (실패 코드는 건너뜀)
- fetch_fund_quote(page, code): 단일 코드 조회
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone

log = logging.getLogger(__name__)

BASE_URL = "https://www.fundguide.net/Fund/SimpleSearch?search_key={code}"
NAV_SELECTOR = 'td.taC[data-tab="tab0"]'
RESULT_TAB = 'a:has-text("검색결과"), button:has-text("검색결과")'   # 클릭해야 결과 그리드가 렌더됨
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)


@dataclass
class FundQuote:
    fund_cd: str
    fund_nm: str | None
    nav: float | None
    change: float | None
    crawled_at: str


def _parse_number(text: str) -> float | None:
    """
    Purpose: 문자열에서 첫 숫자 추출
    Parameter:
        - text: 화면 텍스트
    Input: 없음
    Output: float 또는 None
    """
    m = re.search(r"-?\d[\d,]*\.?\d*", text or "")
    return float(m.group(0).replace(",", "")) if m else None


def fetch_fund_quote(page, code: str, timeout_ms: int = 30_000) -> FundQuote:
    """
    Purpose: 펀드 1개 기준가 조회
    Parameter:
        - page: Playwright Page
        - code: 펀드 표준코드
        - timeout_ms: 대기 한도 (기본 30초)
    Input: fundguide.net
    Output: FundQuote (렌더 대기 초과 시 Playwright TimeoutError)
    """
    page.goto(BASE_URL.format(code=code), wait_until="networkidle", timeout=timeout_ms)
    tab = page.locator(RESULT_TAB).first
    try:
        if tab.count() and tab.is_visible():
            tab.click()
    except Exception:                            # 탭 없음·이미 활성 → 무시
        pass
    page.wait_for_selector(NAV_SELECTOR, timeout=timeout_ms, state="visible")
    row = page.locator(NAV_SELECTOR).first
    nav_text = row.evaluate("el => el.childNodes[0]?.textContent || ''").strip()
    change_text = row.locator("span.tcr").inner_text() if row.locator("span.tcr").count() else ""
    chk = page.locator(f'input[data-fund-cd="{code}"]').first
    return FundQuote(
        fund_cd=code,
        fund_nm=chk.get_attribute("data-fund-nm") if chk.count() else None,
        nav=_parse_number(nav_text),
        change=_parse_number(change_text),
        crawled_at=datetime.now(timezone.utc).isoformat(),
    )


def crawl_fund_prices(codes: list[str], standard_date: str) -> list[dict]:
    """
    Purpose: 여러 펀드 기준가 크롤링 (브라우저 1개 공유)
    Parameter:
        - codes: 펀드 표준코드 리스트
        - standard_date: 레코드 기준일
    Input: fundguide.net (Chromium)
    Output: [{standard_date, product_code, product_name, standard_price}] — NAV 없음·오류 코드는 로그 후 제외
    """
    from playwright.sync_api import sync_playwright

    records = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_context(user_agent=USER_AGENT, locale="ko-KR").new_page()
        for code in codes:
            try:
                quote = fetch_fund_quote(page, code)
            except Exception as e:                # 타임아웃 포함 — 코드 단위 격리
                log.warning("[SKIP] %s: %s", code, type(e).__name__)
                continue
            if quote.nav is None:
                log.warning("[SKIP] %s — nav 없음", code)
                continue
            records.append({"standard_date": standard_date, "product_code": code,
                            "product_name": quote.fund_nm, "standard_price": quote.nav})
            log.info("[OK] %s nav=%s", code, quote.nav)
        browser.close()
    return records
