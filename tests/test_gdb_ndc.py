"""GDB NDC client — 單元測試（無網路）"""
import base64
import hashlib
import hmac
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest

from integrations.gdb_ndc import (
    GDBClient,
    GDBConfig,
    SearchSpec,
    dataset_dsno,
    dataset_label,
    detect_field,
    detect_name_field,
    find_plan_datasets,
    search_plans,
    unwrap_list,
)


# ─── HMAC 簽章 ──────────────────────────────────────────────


def test_build_auth_headers_matches_reference():
    """HMAC-SHA256 簽章必須符合 GDB 規範格式。"""
    app_id = "demo-app-id"
    app_key = "super-secret-key"
    x_date = "Tue, 18 May 2026 06:25:24 GMT"

    headers = GDBClient.build_auth_headers(app_id, app_key, x_date=x_date)

    expected_digest = hmac.new(
        app_key.encode("utf-8"),
        f"x-date: {x_date}".encode("utf-8"),
        hashlib.sha256,
    ).digest()
    expected_sig = base64.b64encode(expected_digest).decode("ascii")

    assert headers["x-date"] == x_date
    assert f'signature="{expected_sig}"' in headers["Authorization"]
    assert f'username="{app_id}"' in headers["Authorization"]
    assert 'algorithm="hmac-sha256"' in headers["Authorization"]
    assert 'headers="x-date"' in headers["Authorization"]
    assert headers["Accept"] == "application/json"


def test_build_auth_headers_default_xdate_is_gmt():
    headers = GDBClient.build_auth_headers("id", "key")
    assert headers["x-date"].endswith("GMT"), headers["x-date"]


def test_client_requires_credentials():
    with pytest.raises(ValueError):
        GDBClient(GDBConfig(app_id="", app_key=""))
    with pytest.raises(ValueError):
        GDBClient(GDBConfig(app_id="x", app_key=""))


# ─── unwrap_list ────────────────────────────────────────────


def test_unwrap_list_handles_direct_list():
    assert unwrap_list([{"a": 1}, {"b": 2}]) == [{"a": 1}, {"b": 2}]


def test_unwrap_list_handles_common_wrappers():
    assert unwrap_list({"data": [{"a": 1}]}) == [{"a": 1}]
    assert unwrap_list({"Result": [{"x": 1}]}) == [{"x": 1}]
    assert unwrap_list({"DatasetList": [{"Dsno": "001"}]}) == [{"Dsno": "001"}]
    assert unwrap_list({"rows": [{"y": 2}]}) == [{"y": 2}]


def test_unwrap_list_handles_nested_dict():
    payload = {"data": {"rows": [{"k": "v"}]}}
    assert unwrap_list(payload) == [{"k": "v"}]


def test_unwrap_list_returns_empty_for_unknown_shape():
    assert unwrap_list({"unknown": "string"}) == []
    assert unwrap_list(None) == []
    assert unwrap_list("not a dict") == []


def test_unwrap_list_filters_non_dict_items():
    assert unwrap_list([{"a": 1}, "string", 42, {"b": 2}]) == [{"a": 1}, {"b": 2}]


# ─── 欄位偵測 ───────────────────────────────────────────────


def test_detect_field_prefers_exact_match():
    row = {"計畫名稱": "x", "ProjectName": "y", "irrelevant": "z"}
    assert detect_field(row, ("計畫名稱", "ProjectName")) == "計畫名稱"


def test_detect_field_case_insensitive():
    row = {"projectname": "x"}
    assert detect_field(row, ("ProjectName",)) == "projectname"


def test_detect_name_field_falls_back_to_longest_string():
    row = {"id": "ABC123", "url": "http://x", "description": "這是一個關於智慧城市的計畫詳細描述"}
    assert detect_name_field(row) == "description"


def test_detect_name_field_skips_id_like_keys():
    row = {"PlanGUID": "0000-0000-1111-2222-3333-44444444", "title": "再生能源"}
    assert detect_name_field(row) == "title"


# ─── dataset 顯示輔助 ───────────────────────────────────────


def test_dataset_label_uses_dataset_name():
    assert "智慧城市計畫" in dataset_label({"DatasetName": "智慧城市計畫", "Dsno": "DS001"})


def test_dataset_dsno_resolves_various_keys():
    assert dataset_dsno({"Dsno": "A"}) == "A"
    assert dataset_dsno({"dsno": "B"}) == "B"
    assert dataset_dsno({"ID": "C"}) == "C"
    assert dataset_dsno({}) == ""


def test_find_plan_datasets_filters_by_plan_keyword():
    pool = [
        {"DatasetName": "智慧城市計畫資料", "Dsno": "1"},
        {"DatasetName": "氣象觀測資料", "Dsno": "2"},
        {"DatasetName": "Government Plan Database", "Dsno": "3"},
    ]
    result = find_plan_datasets(pool)
    assert {dataset_dsno(d) for d in result} == {"1", "3"}


# ─── 搜尋邏輯（用 fake client） ──────────────────────────────


class _FakeClient:
    """模擬 GDBClient 的 iter_dataset；其他方法不需要。"""

    def __init__(self, rows):
        self._rows = rows

    def iter_dataset(self, dsno, page_size=200, max_pages=50):
        yield from self._rows


@pytest.fixture
def sample_rows():
    return [
        {"計畫名稱": "智慧城市發展計畫", "主管機關": "經濟部", "計畫年度": "113", "計畫類別": "公共建設"},
        {"計畫名稱": "智慧交通示範計畫", "主管機關": "交通部", "計畫年度": "113", "計畫類別": "公共建設"},
        {"計畫名稱": "長照2.0補助計畫", "主管機關": "衛福部", "計畫年度": "113", "計畫類別": "社會發展"},
        {"計畫名稱": "再生能源推動計畫", "主管機關": "經濟部", "計畫年度": "112", "計畫類別": "經濟發展"},
        {"計畫名稱": "智慧農業計畫", "主管機關": "農業部", "計畫年度": "114", "計畫類別": "經濟發展"},
    ]


def test_search_keyword_only(sample_rows):
    client = _FakeClient(sample_rows)
    result = search_plans(client, "DS", SearchSpec(keyword="智慧"))
    names = [r["計畫名稱"] for r in result.rows]
    assert names == ["智慧城市發展計畫", "智慧交通示範計畫", "智慧農業計畫"]
    assert result.detected_fields["name"] == "計畫名稱"


def test_search_with_agency(sample_rows):
    client = _FakeClient(sample_rows)
    result = search_plans(client, "DS", SearchSpec(keyword="智慧", agency="經濟部"))
    names = [r["計畫名稱"] for r in result.rows]
    assert names == ["智慧城市發展計畫"]


def test_search_with_year_and_category(sample_rows):
    client = _FakeClient(sample_rows)
    result = search_plans(client, "DS", SearchSpec(keyword="計畫", year="113", category="公共建設"))
    names = [r["計畫名稱"] for r in result.rows]
    assert names == ["智慧城市發展計畫", "智慧交通示範計畫"]


def test_search_keyword_case_insensitive():
    rows = [{"PlanName": "Green Energy Project"}, {"PlanName": "Other"}]
    client = _FakeClient(rows)
    result = search_plans(client, "DS", SearchSpec(keyword="green"))
    assert [r["PlanName"] for r in result.rows] == ["Green Energy Project"]


def test_search_fallback_scans_all_string_fields():
    """關鍵字不在偵測到的名稱欄位中，但出現在其他欄位 → 仍應命中。"""
    rows = [
        {"PlanName": "Generic Title", "Description": "與半導體相關的計畫"},
        {"PlanName": "Unrelated", "Description": "其他主題"},
    ]
    client = _FakeClient(rows)
    result = search_plans(client, "DS", SearchSpec(keyword="半導體"))
    assert len(result.rows) == 1
    assert result.rows[0]["PlanName"] == "Generic Title"


def test_search_respects_max_rows():
    rows = [{"計畫名稱": f"計畫 {i}"} for i in range(20)]
    client = _FakeClient(rows)
    result = search_plans(client, "DS", SearchSpec(keyword="計畫"), max_rows=5)
    assert result.truncated is True
    assert len(result.rows) == 5


def test_search_empty_keyword_returns_all(sample_rows):
    client = _FakeClient(sample_rows)
    result = search_plans(client, "DS", SearchSpec(keyword=""))
    assert len(result.rows) == len(sample_rows)


def test_search_no_match_returns_empty(sample_rows):
    client = _FakeClient(sample_rows)
    result = search_plans(client, "DS", SearchSpec(keyword="ZZZ_NEVER_MATCH"))
    assert result.rows == []
    assert result.scanned == len(sample_rows)
