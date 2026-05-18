"""政府計畫資料庫 (GDB) OpenAPI 客戶端

來源
----
- 官網: https://gdb.ndc.gov.tw/
- OpenAPI / Swagger: https://gdb.ndc.gov.tw/swagger/
- 使用手冊: https://gdb-ndcgov.gitbook.io/gdb-web/api/api-operating-manual

認證
----
GDB OpenAPI 採 HMAC-SHA256 簽章，需先至官網申請 APP ID / APP Key。

簽章流程（簽章效期 5 分鐘）：

    x-date  = 目前時間, RFC1123 GMT, e.g. "Tue, 18 May 2026 06:25:24 GMT"
    string_to_sign = "x-date: " + x_date
    signature = base64( hmac_sha256(APP_Key, string_to_sign) )
    Authorization  = 'hmac username="<APP_ID>", algorithm="hmac-sha256", '
                     'headers="x-date", signature="<signature>"'

端點（依官方 Swagger）
---------------------
- GET /DatasetList                 列出可用資料集
- GET /SchemaInfoApi/{Dsno}        取得指定資料集欄位結構
- GET /Dataset/{Dsno}              取得資料集內容（支援分頁）

實際 base path 由 Swagger 為準（常見為空字串或 /api/v1），可透過 GDBConfig.base_path 調整。
"""

from __future__ import annotations

import base64
import hashlib
import hmac
from dataclasses import dataclass, field
from email.utils import formatdate
from typing import Any, Dict, Iterable, Iterator, List, Optional

import requests


DEFAULT_HOST = "https://gdb.ndc.gov.tw"
DEFAULT_BASE_PATH = ""  # Swagger 端點直接掛在 host 根目錄；如需 /api/v1 可調整


_NAME_FIELD_CANDIDATES = (
    "計畫名稱", "計畫項目名稱", "計畫項目", "計畫",
    "PlanName", "ProjectName", "PlanTitle", "ProjectTitle", "Name",
    "plan_name", "project_name",
)

_AGENCY_FIELD_CANDIDATES = (
    "主管機關", "主辦機關", "主管單位", "主辦單位", "執行機關", "機關名稱", "機關",
    "Agency", "Department",
)

_YEAR_FIELD_CANDIDATES = (
    "計畫年度", "年度", "執行年度", "Year", "FiscalYear",
)

_CATEGORY_FIELD_CANDIDATES = (
    "計畫類別", "類別", "計畫類型", "Category", "PlanCategory",
)


# ─── 設定 / 例外 ──────────────────────────────────────────────


@dataclass
class GDBConfig:
    """GDB 連線設定。"""

    app_id: str
    app_key: str
    host: str = DEFAULT_HOST
    base_path: str = DEFAULT_BASE_PATH
    timeout: int = 30


class GDBAuthError(RuntimeError):
    """HMAC 簽章驗證失敗或回傳非 JSON。"""


# ─── 客戶端 ──────────────────────────────────────────────────


class GDBClient:
    """GDB OpenAPI 用戶端（HMAC-SHA256 簽章）。

    Example:
        >>> cfg = GDBConfig(app_id="...", app_key="...")
        >>> client = GDBClient(cfg)
        >>> datasets = client.list_datasets()
        >>> rows = list(client.iter_dataset(dsno, max_pages=5))
    """

    def __init__(self, config: GDBConfig, session: Optional[requests.Session] = None):
        if not config.app_id or not config.app_key:
            raise ValueError("APP ID / APP Key 不可為空")
        self.config = config
        self.session = session or requests.Session()

    # ── 簽章 ────────────────────────────────────────────────
    @staticmethod
    def build_auth_headers(app_id: str, app_key: str, x_date: Optional[str] = None) -> Dict[str, str]:
        """產生符合 GDB 規範的 Authorization / x-date headers。

        Args:
            app_id: APP ID（會放入 username 欄位）
            app_key: APP Key（HMAC 簽章金鑰）
            x_date: 自訂 GMT 時間字串；預設為當下 UTC 時間。
        """
        if x_date is None:
            x_date = formatdate(timeval=None, usegmt=True)
        string_to_sign = f"x-date: {x_date}"
        digest = hmac.new(
            app_key.encode("utf-8"),
            string_to_sign.encode("utf-8"),
            hashlib.sha256,
        ).digest()
        signature = base64.b64encode(digest).decode("ascii")
        authorization = (
            f'hmac username="{app_id}", '
            f'algorithm="hmac-sha256", '
            f'headers="x-date", '
            f'signature="{signature}"'
        )
        return {
            "x-date": x_date,
            "Authorization": authorization,
            "Accept": "application/json",
        }

    def _headers(self) -> Dict[str, str]:
        return self.build_auth_headers(self.config.app_id, self.config.app_key)

    # ── 低階呼叫 ────────────────────────────────────────────
    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        url = f"{self.config.host.rstrip('/')}{self.config.base_path}{path}"
        resp = self.session.get(
            url,
            headers=self._headers(),
            params=params or {},
            timeout=self.config.timeout,
        )
        if resp.status_code in (401, 403):
            raise GDBAuthError(
                f"HMAC 認證失敗 (HTTP {resp.status_code})：請確認 APP ID / APP Key 正確且未過期。"
                f"\n回應: {resp.text[:300]}"
            )
        resp.raise_for_status()
        ctype = resp.headers.get("Content-Type", "")
        if "json" not in ctype.lower():
            raise GDBAuthError(f"非 JSON 回應 ({ctype})：{resp.text[:300]}")
        return resp.json()

    # ── 高階 API ────────────────────────────────────────────
    def list_datasets(self) -> List[Dict[str, Any]]:
        """列出所有可存取的資料集。"""
        return unwrap_list(self._get("/DatasetList"))

    def get_schema(self, dsno: str) -> List[Dict[str, Any]]:
        """取得指定資料集的欄位結構。"""
        return unwrap_list(self._get(f"/SchemaInfoApi/{dsno}"))

    def query_dataset(
        self,
        dsno: str,
        page_num: int = 1,
        page_size: int = 100,
        **extra_params: Any,
    ) -> Any:
        """單頁查詢資料集內容（原始回傳）。"""
        params: Dict[str, Any] = {"pageNum": page_num, "pageSize": page_size}
        params.update(extra_params)
        return self._get(f"/Dataset/{dsno}", params=params)

    def iter_dataset(
        self,
        dsno: str,
        page_size: int = 200,
        max_pages: int = 50,
        **extra_params: Any,
    ) -> Iterator[Dict[str, Any]]:
        """依分頁逐筆 yield 資料；遇到空頁或不足整頁時自動停止。"""
        for page in range(1, max_pages + 1):
            payload = self.query_dataset(
                dsno, page_num=page, page_size=page_size, **extra_params
            )
            rows = unwrap_list(payload)
            if not rows:
                break
            for row in rows:
                yield row
            if len(rows) < page_size:
                break


# ─── JSON 解包 ────────────────────────────────────────────────


def unwrap_list(payload: Any) -> List[Dict[str, Any]]:
    """常見 API 回傳格式統一轉成 list[dict]。

    支援：
    - list 直接是 [ {...}, ... ]
    - dict 包 {"data": [...]}, {"result": [...]}, {"rows": [...]}, {"DatasetList": [...]}, etc.
    """
    if isinstance(payload, list):
        return [r for r in payload if isinstance(r, dict)]
    if isinstance(payload, dict):
        for key in ("data", "Data", "result", "Result", "items", "Items",
                    "rows", "Rows", "list", "List", "DatasetList"):
            value = payload.get(key)
            if isinstance(value, list):
                return [r for r in value if isinstance(r, dict)]
            if isinstance(value, dict):
                # nested e.g. {"data": {"rows": [...]}}
                return unwrap_list(value)
    return []


# ─── 欄位偵測 ────────────────────────────────────────────────


def detect_field(row: Dict[str, Any], candidates: Iterable[str]) -> Optional[str]:
    """從 row 中找出第一個符合候選名單的欄位名稱。"""
    keys = list(row.keys())
    keys_lc = {k.casefold(): k for k in keys}
    for cand in candidates:
        if cand in row:
            return cand
        lc = cand.casefold()
        if lc in keys_lc:
            return keys_lc[lc]
    return None


def detect_name_field(row: Dict[str, Any]) -> Optional[str]:
    """偵測「計畫名稱」欄位；找不到時退而求其次挑最長字串欄位。"""
    found = detect_field(row, _NAME_FIELD_CANDIDATES)
    if found:
        return found
    longest_key: Optional[str] = None
    longest_len = 0
    for k, v in row.items():
        if not isinstance(v, str):
            continue
        kl = k.lower()
        if any(skip in kl for skip in ("url", "guid", "uuid", "id", "code")):
            continue
        if len(v) > longest_len:
            longest_len = len(v)
            longest_key = k
    return longest_key


# ─── 搜尋 ────────────────────────────────────────────────────


@dataclass
class SearchSpec:
    """計畫搜尋參數。

    keyword 為主要計畫名稱關鍵字；agency / year / category 為可選輔助篩選。
    所有比對均為大小寫不敏感的「子字串包含」。
    """

    keyword: str = ""
    agency: str = ""
    year: str = ""
    category: str = ""
    keyword_field: str = ""
    agency_field: str = ""
    year_field: str = ""
    category_field: str = ""


@dataclass
class SearchResult:
    rows: List[Dict[str, Any]] = field(default_factory=list)
    detected_fields: Dict[str, str] = field(default_factory=dict)
    scanned: int = 0
    truncated: bool = False


def _match(value: Any, keyword_lc: str) -> bool:
    return keyword_lc in str(value).casefold() if keyword_lc else True


def search_plans(
    client: GDBClient,
    dsno: str,
    spec: SearchSpec,
    *,
    page_size: int = 200,
    max_pages: int = 50,
    max_rows: int = 5000,
) -> SearchResult:
    """對指定資料集執行關鍵字搜尋。

    流程：
    1. 對每一筆資料，先嘗試 `keyword` 在「計畫名稱欄位」做子字串比對；
       若該欄位無 hit，會回退掃描整筆 row 的所有字串值。
    2. 通過後再依 agency / year / category 三個輔助條件做 AND 篩選。
    3. 任一條件留空就跳過該條件。
    """
    keyword_lc = spec.keyword.casefold().strip()
    agency_lc = spec.agency.casefold().strip()
    year_lc = spec.year.casefold().strip()
    category_lc = spec.category.casefold().strip()

    detected: Dict[str, str] = {}
    result = SearchResult()

    for idx, row in enumerate(client.iter_dataset(dsno, page_size=page_size, max_pages=max_pages)):
        if idx >= max_rows:
            result.truncated = True
            break
        result.scanned = idx + 1

        if not detected:
            detected = {
                "name": spec.keyword_field or detect_field(row, _NAME_FIELD_CANDIDATES) or "",
                "agency": spec.agency_field or detect_field(row, _AGENCY_FIELD_CANDIDATES) or "",
                "year": spec.year_field or detect_field(row, _YEAR_FIELD_CANDIDATES) or "",
                "category": spec.category_field or detect_field(row, _CATEGORY_FIELD_CANDIDATES) or "",
            }
            if not detected["name"]:
                detected["name"] = detect_name_field(row) or ""

        # 關鍵字：先試名稱欄位，失敗時退回掃整筆
        if keyword_lc:
            name_field = detected.get("name") or ""
            hit = _match(row.get(name_field, ""), keyword_lc) if name_field else False
            if not hit:
                hit = any(_match(v, keyword_lc) for v in row.values() if isinstance(v, str))
            if not hit:
                continue

        # 其他輔助條件
        if agency_lc and not _match(row.get(detected.get("agency", ""), ""), agency_lc):
            continue
        if year_lc and not _match(row.get(detected.get("year", ""), ""), year_lc):
            continue
        if category_lc and not _match(row.get(detected.get("category", ""), ""), category_lc):
            continue

        result.rows.append(row)

    result.detected_fields = detected
    return result


# ─── 資料集挑選輔助 ─────────────────────────────────────────


def find_plan_datasets(datasets: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """從 /DatasetList 結果中過濾出名稱看起來和「計畫」相關的資料集。"""
    out: List[Dict[str, Any]] = []
    for ds in datasets:
        name = " ".join(str(v) for v in ds.values() if isinstance(v, str))
        if "計畫" in name or "plan" in name.lower() or "project" in name.lower():
            out.append(ds)
    return out


def dataset_label(ds: Dict[str, Any]) -> str:
    """產生資料集的顯示名稱：優先用中文名稱欄位，退而求其次用 Dsno + 任何字串欄位。"""
    for key in ("DatasetName", "Name", "Title", "資料集名稱", "資料集"):
        v = ds.get(key)
        if isinstance(v, str) and v.strip():
            dsno = ds.get("Dsno") or ds.get("dsno") or ds.get("ID") or ""
            return f"{v}  ({dsno})" if dsno else v
    return str(ds.get("Dsno") or ds.get("dsno") or ds)


def dataset_dsno(ds: Dict[str, Any]) -> str:
    for key in ("Dsno", "dsno", "DSNO", "ID", "id"):
        v = ds.get(key)
        if v:
            return str(v)
    return ""
