"""GDB 政府計畫資料庫搜尋頁面

依「計畫名稱關鍵字」搜尋 NDC GDB 資料集，並可加上主管機關、計畫年度、計畫類別篩選。

啟動方式：
  streamlit run pages/02_gdb_search.py        # 單獨啟動
  streamlit run tv_app.py                      # 側邊欄選 "gdb search"

操作步驟：
  1. 申請 APP ID / APP Key (https://gdb.ndc.gov.tw/)
  2. 將 ID / Key 填入側邊欄，或設定環境變數 GDB_APP_ID / GDB_APP_KEY
  3. 載入資料集清單 → 選擇要搜尋的資料集
  4. 輸入計畫名稱關鍵字（+ 其他篩選條件） → 搜尋
  5. 下載 CSV / Excel
"""

import io
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import streamlit as st

from integrations.gdb_ndc import (
    GDBAuthError,
    GDBClient,
    GDBConfig,
    SearchSpec,
    dataset_dsno,
    dataset_label,
    find_plan_datasets,
    search_plans,
)


st.set_page_config(
    page_title="GDB 計畫搜尋 | Stock001",
    page_icon="🗂️",
    layout="wide",
)


# ─── 側邊欄：API 憑證 ───────────────────────────────────────────


def _credential_inputs() -> GDBConfig | None:
    st.sidebar.header("GDB API 憑證")
    st.sidebar.caption(
        "至 [gdb.ndc.gov.tw](https://gdb.ndc.gov.tw/) 申請 APP ID / APP Key，"
        "或設定環境變數 `GDB_APP_ID` / `GDB_APP_KEY`。"
    )
    app_id = st.sidebar.text_input(
        "APP ID",
        value=os.getenv("GDB_APP_ID", ""),
        type="default",
    )
    app_key = st.sidebar.text_input(
        "APP Key",
        value=os.getenv("GDB_APP_KEY", ""),
        type="password",
    )
    with st.sidebar.expander("進階設定"):
        host = st.text_input("Host", value="https://gdb.ndc.gov.tw")
        base_path = st.text_input(
            "Base Path",
            value="",
            help="若 Swagger 顯示端點是 `/api/v1/DatasetList`，就填 `/api/v1`；空白代表掛在根目錄。",
        )
        timeout = st.number_input("Timeout (秒)", min_value=5, max_value=120, value=30, step=5)
    if not app_id or not app_key:
        st.sidebar.warning("請先輸入 APP ID / APP Key。")
        return None
    return GDBConfig(
        app_id=app_id.strip(),
        app_key=app_key.strip(),
        host=host.strip(),
        base_path=base_path.strip(),
        timeout=int(timeout),
    )


# ─── 資料集載入（cached） ─────────────────────────────────────


@st.cache_data(ttl=600, show_spinner=False)
def _load_datasets(app_id: str, app_key: str, host: str, base_path: str, timeout: int):
    client = GDBClient(GDBConfig(app_id, app_key, host, base_path, timeout))
    return client.list_datasets()


def _render_dataset_picker(cfg: GDBConfig) -> str | None:
    col1, col2 = st.columns([3, 1])
    with col2:
        if st.button("重新載入資料集清單", use_container_width=True):
            _load_datasets.clear()

    try:
        datasets = _load_datasets(cfg.app_id, cfg.app_key, cfg.host, cfg.base_path, cfg.timeout)
    except GDBAuthError as exc:
        st.error(f"❌ 認證失敗：{exc}")
        return None
    except Exception as exc:  # noqa: BLE001
        st.error(f"❌ 載入資料集清單失敗：{exc}")
        return None

    if not datasets:
        st.warning("API 沒有回傳任何資料集，請確認 base path 或聯絡 NDC。")
        return None

    plan_only = st.checkbox("只顯示與「計畫」相關的資料集", value=True)
    pool = find_plan_datasets(datasets) if plan_only else datasets
    if not pool:
        st.info("找不到名稱含「計畫」的資料集，請取消勾選查看完整清單。")
        return None

    options = {dataset_label(ds): dataset_dsno(ds) for ds in pool}
    options = {k: v for k, v in options.items() if v}

    with col1:
        chosen_label = st.selectbox(
            f"資料集 ({len(options)} 筆可選)",
            list(options.keys()),
            key="gdb_dataset_label",
        )
    return options.get(chosen_label) if chosen_label else None


# ─── 搜尋表單 ────────────────────────────────────────────────


def _render_search_form() -> tuple[SearchSpec, dict]:
    with st.form("gdb_search_form"):
        st.subheader("搜尋條件")

        keyword = st.text_input(
            "計畫名稱關鍵字（必填）",
            value="",
            placeholder="例如：智慧城市 / 長照 / 再生能源",
        )

        c1, c2, c3 = st.columns(3)
        agency = c1.text_input("主管 / 主辦機關", value="", placeholder="例如：經濟部")
        year = c2.text_input("計畫年度", value="", placeholder="例如：113")
        category = c3.text_input("計畫類別", value="", placeholder="例如：公共建設")

        with st.expander("資料抓取上限"):
            d1, d2, d3 = st.columns(3)
            page_size = d1.number_input("每頁筆數", min_value=10, max_value=1000, value=200, step=50)
            max_pages = d2.number_input("最多抓幾頁", min_value=1, max_value=200, value=25, step=5)
            max_rows = d3.number_input("最多掃幾筆", min_value=100, max_value=50000, value=5000, step=500)

        submitted = st.form_submit_button("🔍 搜尋")

    return (
        SearchSpec(
            keyword=keyword,
            agency=agency,
            year=year,
            category=category,
        ),
        dict(
            submitted=submitted,
            page_size=int(page_size),
            max_pages=int(max_pages),
            max_rows=int(max_rows),
        ),
    )


# ─── 結果呈現 / 匯出 ────────────────────────────────────────


def _export_buttons(df: pd.DataFrame, keyword: str):
    safe_keyword = (keyword or "all").replace("/", "_").replace(" ", "_") or "all"
    c1, c2 = st.columns(2)
    csv_bytes = df.to_csv(index=False).encode("utf-8-sig")
    c1.download_button(
        "⬇️ 下載 CSV",
        data=csv_bytes,
        file_name=f"gdb_plans_{safe_keyword}.csv",
        mime="text/csv",
        use_container_width=True,
    )
    xlsx_buf = io.BytesIO()
    with pd.ExcelWriter(xlsx_buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="plans")
    c2.download_button(
        "⬇️ 下載 Excel",
        data=xlsx_buf.getvalue(),
        file_name=f"gdb_plans_{safe_keyword}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )


def _render_results(cfg: GDBConfig, dsno: str, spec: SearchSpec, opts: dict):
    client = GDBClient(cfg)
    with st.spinner(f"正在從資料集 {dsno} 抓資料並比對關鍵字..."):
        try:
            result = search_plans(
                client,
                dsno,
                spec,
                page_size=opts["page_size"],
                max_pages=opts["max_pages"],
                max_rows=opts["max_rows"],
            )
        except GDBAuthError as exc:
            st.error(f"❌ 認證或解析失敗：{exc}")
            return
        except Exception as exc:  # noqa: BLE001
            st.error(f"❌ 查詢失敗：{exc}")
            return

    detected = result.detected_fields
    summary_parts = [
        f"掃描 **{result.scanned}** 筆，命中 **{len(result.rows)}** 筆",
    ]
    if detected.get("name"):
        summary_parts.append(f"計畫名稱欄位：`{detected['name']}`")
    if detected.get("agency"):
        summary_parts.append(f"機關欄位：`{detected['agency']}`")
    if detected.get("year"):
        summary_parts.append(f"年度欄位：`{detected['year']}`")
    if detected.get("category"):
        summary_parts.append(f"類別欄位：`{detected['category']}`")
    st.info(" ｜ ".join(summary_parts))
    if result.truncated:
        st.warning("⚠️ 已達掃描上限（max_rows），結果可能不完整；請放寬「資料抓取上限」或加上更精確的關鍵字。")

    if not result.rows:
        st.warning("找不到符合條件的計畫。請放寬關鍵字、檢查欄位偵測是否正確，或改選其他資料集。")
        return

    df = pd.DataFrame(result.rows)

    # 把命中的欄位排前面
    front = [v for v in [detected.get("name"), detected.get("agency"),
                          detected.get("year"), detected.get("category")] if v and v in df.columns]
    rest = [c for c in df.columns if c not in front]
    df = df[front + rest]

    st.dataframe(df, use_container_width=True, height=480)
    _export_buttons(df, spec.keyword)


# ─── 主流程 ─────────────────────────────────────────────────


def main():
    st.title("🗂️ GDB 政府計畫搜尋")
    st.caption(
        "依關鍵字搜尋 NDC [政府計畫資料庫](https://gdb.ndc.gov.tw/) 的計畫資料。"
        "工具透過官方 OpenAPI 取得資料，需自備 APP ID / APP Key。"
    )

    cfg = _credential_inputs()
    if cfg is None:
        st.info("👈 請先在左側輸入 APP ID / APP Key。")
        st.markdown(
            "**申請步驟：**\n"
            "1. 至 https://gdb.ndc.gov.tw/ 註冊帳號\n"
            "2. 申請 OpenAPI 使用權，取得 APP ID / APP Key\n"
            "3. 詳見 [API 使用說明](https://gdb-ndcgov.gitbook.io/gdb-web/api/api-operating-manual)"
        )
        return

    dsno = _render_dataset_picker(cfg)
    if not dsno:
        return

    spec, opts = _render_search_form()
    if not opts["submitted"]:
        return

    if not spec.keyword.strip():
        st.warning("請輸入計畫名稱關鍵字。")
        return

    _render_results(cfg, dsno, spec, opts)


main()
