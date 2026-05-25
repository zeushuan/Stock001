"""data_sources — Stock001 external data fetchers.

Phase 1 (Smart Money MVP) — SEC EDGAR via edgartools:
  - edgar_13f:    13F-HR institutional holdings + 季對季 compare
  - edgar_form4:  Form 4 insider transactions（P/S/M/G/F/A codes）

依規格《聰明錢追蹤模組》(Stock001_SmartMoney_Module_Spec.md) 第二節。
"""
from __future__ import annotations

# SEC 規定須設定識別資訊（否則 EDGAR 會擋）
# 集中設定，所有 edgar_* 模組共用
SEC_IDENTITY = "Eddy Huang eddy.huang@innojetech.com"

_identity_set = False


def ensure_sec_identity() -> None:
    """確保 SEC identity 已設定（每個 process 設一次就好）。"""
    global _identity_set
    if _identity_set:
        return
    try:
        from edgar import set_identity
        set_identity(SEC_IDENTITY)
        _identity_set = True
    except ImportError as e:
        raise ImportError(
            "edgartools 未安裝。執行: pip install edgartools"
        ) from e
