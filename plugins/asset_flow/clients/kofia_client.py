"""
금융투자협회 전자공시(KOFIA) 펀드 기준가격 조회 클라이언트

- 공개 화면이 쓰는 XML 서비스(DISFundStdPrcStutSO), 인증 없음
- get_fund_nav_history(code, start, end): 기준일별 기준가 (기간 제한 없음)
- timeout 은 KOFIA.TIMEOUT (비브라우저 UA 응답 지연 대비, 공통 기본 30초보다 김), 여러 펀드는 호출자가 병렬로 (fund_nav.fetch_all)
- User-Agent 는 브라우저 값 (Python 기본 UA 는 서버가 응답 없이 끊음)
"""

from __future__ import annotations

import time
import xml.etree.ElementTree as ET

from asset_flow.clients.base_client import BaseApiClient
from asset_flow.config.kofia import KOFIA

_BODY = """<?xml version="1.0" encoding="utf-8"?><message><proframeHeader><pfmAppName>{app}</pfmAppName>\
<pfmSvcName>{svc}</pfmSvcName><pfmFnName>select</pfmFnName></proframeHeader><systemHeader></systemHeader>\
<DISCondFuncDTO><tmpV30>{start}</tmpV30><tmpV31>{end}</tmpV31><tmpV10>0</tmpV10><tmpV12>{code}</tmpV12>\
</DISCondFuncDTO></message>"""


class KofiaClient(BaseApiClient):
    """KOFIA 전자공시 XML 서비스 클라이언트."""

    def __init__(self):
        """
        Purpose: 기본 URL 설정
        Parameter: 없음
        Input: 없음
        Output: 없음
        """
        super().__init__(base_url=KOFIA.URL)

    def _build_headers(self) -> dict[str, str]:
        """
        Purpose: XML 요청 헤더
        Parameter: 없음
        Input: 없음
        Output: 헤더 dict (브라우저 User-Agent 포함)
        """
        return {"Content-Type": "text/xml; charset=utf-8", "User-Agent": KOFIA.USER_AGENT}

    def get_fund_nav_history(self, code: str, start: str, end: str, interval: float = 0.35) -> list[dict]:
        """
        Purpose: 펀드 기준가 이력 조회
        Parameter:
            - code: 펀드 표준코드 (K55105D43299 등)
            - start / end: 기준일 YYYYMMDD (양끝 포함)
            - interval: 호출 전 대기 초 (기본 0.35, fetch_all 병렬 호출이면 스레드마다 동시에 대기 — 간격 효과 없음)
        Input: dis.kofia.or.kr
        Output: [{"standard_date": "YYYYMMDD", "standard_price": "1755.96"}] — 기준일만(주말·휴장 없음), 없으면 []
        """
        time.sleep(interval)
        body = _BODY.format(app=KOFIA.APP_NAME, svc=KOFIA.FUND_NAV_SERVICE, start=start, end=end, code=code)
        resp = self.safe_request("POST", self.base_url, headers=self._build_headers(), data=body.encode("utf-8"),
                                 timeout=KOFIA.TIMEOUT)
        root = ET.fromstring(resp.content)
        return [{"standard_date": m.findtext("tmpV1"), "standard_price": m.findtext("tmpV2")}
                for m in root.iter("selectMeta") if m.findtext("tmpV12") == code and m.findtext("tmpV1")]
