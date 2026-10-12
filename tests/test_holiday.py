from asset_flow import holiday as h


def test_translate_split_and_observed():
    assert h.translate_us("Thanksgiving Day") == "추수감사절"
    assert h.translate_us("Independence Day (observed)") == "독립기념일 대체 휴장"
    assert h.translate_us("New Year's Day; Christmas Day") == "신정; 크리스마스"
    assert h.translate_us("Unknown Day") == "Unknown Day"


def test_kasi_items_normalization():
    one = {"response": {"body": {"items": {"item": {"locdate": 20261009, "dateName": "한글날", "isHoliday": "Y"}}}}}
    many = {"response": {"body": {"items": {"item": [
        {"locdate": 20261003, "dateName": "개천절", "isHoliday": "Y"},      # 토요일 → 제외
        {"locdate": 20261005, "dateName": "대체공휴일", "isHoliday": "Y"},
        {"locdate": 20261006, "dateName": "기념일", "isHoliday": "N"},
    ]}}}}
    empty = {"response": {"body": {"items": ""}}}
    assert len(h.kasi_items(one)) == 1 and len(h.kasi_items(many)) == 3 and h.kasi_items(empty) == []
    assert h.kasi_records(h.kasi_items(one)) == [["KR", "2026-10-09", "한글날", "kasi"]]
    assert h.kasi_records(h.kasi_items(many)) == [["KR", "2026-10-05", "대체공휴일", "kasi"]]


def test_library_2026_counts_and_weekdays_only():
    recs = h.library_records(2026)
    y2026 = [r for r in recs if r[1].startswith("2026")]
    assert sum(r[0] == "KR" for r in y2026) == 17                       # 2026 기대값
    assert sum(r[0] == "US" for r in y2026) == 10
    assert [r for r in y2026 if r[1] == "2026-07-03"][0][:3] == ["US", "2026-07-03", "독립기념일 대체 휴장"]
    from datetime import date
    assert all(date.fromisoformat(r[1]).weekday() < 5 for r in recs)
    assert h.year_range(2026) == ("2025-01-01", "2027-12-31")


def test_desired_rows_library_first_and_manual_protected():
    lib = [["KR", "2026-10-09", "한글날", "holidays 0.106"]]
    kasi = [["KR", "2026-10-09", "한글날(KASI)", "kasi"], ["KR", "2026-11-02", "임시공휴일", "kasi"],
            ["KR", "2026-12-24", "수동과 겹침", "kasi"]]
    rows = h.desired_rows(lib, kasi, manual_keys={("KR", "2026-12-24")})
    assert rows == {("KR", "2026-10-09"): ("한글날", "holidays 0.106"), ("KR", "2026-11-02"): ("임시공휴일", "kasi")}


def test_diff_rows_detects_add_remove_rename_and_no_change():
    cur = {("KR", "2026-10-09"): ("한글날", "x"), ("US", "2026-07-03"): ("독립기념일", "x")}
    want = {("KR", "2026-10-09"): ("한글날", "x"), ("KR", "2026-11-02"): ("임시공휴일", "kasi"),
            ("US", "2026-07-03"): ("독립기념일 대체 휴장", "x")}
    assert h.diff_rows(cur, want) == {"added": ["KR 2026-11-02 임시공휴일"], "removed": [],
                                      "renamed": ["US 2026-07-03 독립기념일 → 독립기념일 대체 휴장"]}
    assert not any(h.diff_rows(cur, dict(cur)).values())
