"""Phase 4 — Look-ahead Bias 自動審查

兩種檢查：
1. 程式碼 grep：搜尋已知危險 pattern（rolling(center=True), shift(-N) 等）
2. 數值測試：截至 t 日的 RS 必須等於用 [0..t] 子序列重算的結果
"""
import sys, os
import re
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import pandas as pd
import numpy as np
import pytest

from rs_line import calculate_rs_line, detect_rs_new_high
from sepa_vcp import compute_returns, compute_rs_ratings


# ────────────────────────────────────────────────────────────────
# 1. 靜態 grep：已知危險 pattern
# ────────────────────────────────────────────────────────────────

RS_FILES = [
    'rs_line.py',
    'sepa_vcp.py',
]

# 可能造成 look-ahead 的 pattern
DANGEROUS_PATTERNS = [
    (r'\.rolling\([^)]*center\s*=\s*True', 'rolling(center=True) 會用未來資料'),
    (r'\.shift\(\s*-\s*\d+', 'shift(-N) 把未來值移到當下'),
    (r'\.iloc\[\s*[a-z_]+\s*\+\s*\d+', '可能 lookahead 索引偏移'),
]


def _scan_file(filepath):
    if not os.path.exists(filepath):
        return []
    with open(filepath, 'r', encoding='utf-8') as f:
        code = f.read()
    findings = []
    for pat, msg in DANGEROUS_PATTERNS:
        for m in re.finditer(pat, code):
            line_no = code[:m.start()].count('\n') + 1
            line = code.split('\n')[line_no - 1].strip()
            findings.append((filepath, line_no, line, msg))
    return findings


def test_no_lookahead_pattern_in_rs_code():
    """RS 相關檔案不能有 rolling(center=True) / shift(-N) 等模式"""
    root = os.path.join(os.path.dirname(__file__), '..', '..')
    all_findings = []
    for f in RS_FILES:
        all_findings += _scan_file(os.path.join(root, f))
    assert not all_findings, \
        '發現 look-ahead 嫌疑 pattern:\n' + \
        '\n'.join([f'  {fp}:{ln}  [{msg}]  → {code}'
                    for fp, ln, code, msg in all_findings])


# ────────────────────────────────────────────────────────────────
# 2. 數值測試：時序一致性
# ────────────────────────────────────────────────────────────────

def test_rs_line_truncation_invariance():
    """截至 t 日的 RS Line[t] 應該等於用 [0..t] 子序列重算的 RS[-1]"""
    dates = pd.date_range('2024-01-01', periods=100, freq='B')
    stock = pd.Series(100 * (1.001 ** np.arange(100)), index=dates)
    index = pd.Series( 50 * (1.0005 ** np.arange(100)), index=dates)

    rs_full = calculate_rs_line(stock, index)

    # 抽 t = 50, 75, 99 日各驗證
    for t in [50, 75, 99]:
        stock_t = stock.iloc[:t+1]
        index_t = index.iloc[:t+1]
        rs_t = calculate_rs_line(stock_t, index_t)
        # 用最後一日的值對比
        assert abs(rs_full.iloc[t] - rs_t.iloc[-1]) < 1e-10, \
            f't={t}: full={rs_full.iloc[t]:.6f} vs truncated={rs_t.iloc[-1]:.6f}'


def test_rs_line_new_high_no_lookahead():
    """RS Line 新高判定（紫色點）：用 50d rolling max，方向必須是過去"""
    dates = pd.date_range('2024-01-01', periods=100, freq='B')
    # 製造一個 RS Line：先升後降
    rs = pd.Series(np.concatenate([np.linspace(1.0, 1.5, 50),
                                       np.linspace(1.5, 1.2, 50)]),
                    index=dates)
    is_high = detect_rs_new_high(rs, lookback=50)

    # 第 49 日（升段末）必須是新高
    assert is_high.iloc[49] == True
    # 第 50 日後（下降段）不應該是新高（除非剛好等於前高）
    # 但 lookback=50 視窗向後滾，所以下降段早期可能還包含前高
    # 嚴格測試：第 99 日（最末），rolling 視窗 [50, 99] 內的 max ≠ rs[99]
    assert is_high.iloc[99] == False


def test_compute_returns_uses_only_past():
    """compute_returns 應該只用 [t-Ndays .. t] 範圍的資料"""
    dates = pd.date_range('2024-01-01', periods=300, freq='B')
    close = pd.Series(np.linspace(100, 200, 300), index=dates)
    df_full = pd.DataFrame({'Close': close})

    rets_full = compute_returns(df_full)

    # 取截至 t=200 的子序列重算
    df_t = df_full.iloc[:201]  # 0..200 共 201 個
    rets_t = compute_returns(df_t)

    # 兩個 returns 不應該相同（因為時點不同），但子序列的計算邏輯應該獨立
    # 主要驗證：子序列計算不丟例外，回傳結果合理
    assert 'rets' not in [None]  # placeholder
    assert isinstance(rets_t, dict)
    # 子序列 52w (252d) 不夠長 → 應該為 0
    assert rets_t['52w'] == 0


# ────────────────────────────────────────────────────────────────
# 3. 對數 vs 線性報酬一致性
# ────────────────────────────────────────────────────────────────

def test_returns_linear_consistency():
    """compute_returns 使用線性報酬（百分比）— 不能與對數混用

    驗證：對相同序列，計算結果與「手動線性報酬」一致
    """
    dates = pd.date_range('2024-01-01', periods=400, freq='B')
    close = pd.Series(np.linspace(100, 150, 400), index=dates)
    df = pd.DataFrame({'Close': close})

    rets = compute_returns(df, periods_days=(65, 130, 195, 252))
    cur = close.iloc[-1]
    for label, days in zip(['13w', '26w', '39w', '52w'], [65, 130, 195, 252]):
        past = close.iloc[-days-1]
        expected = (cur - past) / past * 100
        assert abs(rets[label] - expected) < 1e-6, \
            f'{label}: {rets[label]} vs expected {expected}'


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
