"""Phase 3 — RS 邊界案例與公司事件測試"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import pandas as pd
import numpy as np
import pytest

from rs_line import calculate_rs_line
from sepa_vcp import compute_returns


SNAPSHOT_PATH = os.path.join(os.path.dirname(__file__), 'baseline_snapshot.parquet')


@pytest.fixture
def snapshot():
    if not os.path.exists(SNAPSHOT_PATH):
        pytest.skip(f'快照不存在：{SNAPSHOT_PATH}')
    return pd.read_parquet(SNAPSHOT_PATH)


def _extract_close(snapshot, ticker):
    """從多 ticker MultiIndex 快照中抽出單 ticker close 序列"""
    if (ticker, 'Close') in snapshot.columns:
        return snapshot[(ticker, 'Close')].dropna()
    return None


# ────────────────────────────────────────────────────────────────
# 3.1 股票分割（auto_adjust 應該已自動處理）
# ────────────────────────────────────────────────────────────────

def test_split_continuity_nvda(snapshot):
    """NVDA 2024-06-10 10-1 分割。auto_adjust=True 下，前後一日連續。

    驗收：分割日當天的 RS pct_change 絕對值 < 5%
    """
    nvda = _extract_close(snapshot, 'NVDA')
    spx  = _extract_close(snapshot, '^GSPC')
    if nvda is None or spx is None:
        pytest.skip('NVDA 或 ^GSPC 不在快照中')

    rs = calculate_rs_line(nvda, spx)
    split_date = pd.Timestamp('2024-06-10')

    # 找最接近的交易日
    if split_date not in rs.index:
        nearest = rs.index[rs.index.get_indexer([split_date], method='nearest')[0]]
        split_date = nearest

    rs_changes = rs.pct_change()
    delta = rs_changes.loc[split_date]
    assert abs(delta) < 0.05, \
        f'NVDA 分割日 RS 跳動 {delta*100:.2f}% — auto_adjust 失效？'


# ────────────────────────────────────────────────────────────────
# 3.2 交易日對齊（台股 vs 美股）
# ────────────────────────────────────────────────────────────────

def test_calendar_alignment_tsmc(snapshot):
    """2330.TW 對 ^TWII 應該完美對齊（同一個市場）"""
    tsmc = _extract_close(snapshot, '2330.TW')
    twii = _extract_close(snapshot, '^TWII')
    if tsmc is None or twii is None:
        pytest.skip('2330.TW 或 ^TWII 不在快照中')

    rs = calculate_rs_line(tsmc, twii)
    # 必須沒有 NaN
    assert rs.notna().all(), 'RS Line 有 NaN'
    # 索引應該等於交集
    common = tsmc.index.intersection(twii.index)
    assert set(rs.index) <= set(common), 'RS 索引超出交集'
    # 2 年資料，至少 400 個交易日
    assert 400 <= len(rs) <= 520, f'RS 長度異常 ({len(rs)})'


def test_calendar_alignment_cross_market(snapshot):
    """2330.TW 對 ^GSPC 跨市場 — 索引取交集，無 NaN 殘留"""
    tsmc = _extract_close(snapshot, '2330.TW')
    spx  = _extract_close(snapshot, '^GSPC')
    if tsmc is None or spx is None:
        pytest.skip('資料不在快照中')

    rs = calculate_rs_line(tsmc, spx)
    assert rs.notna().all(), '跨市場 RS 仍有 NaN — 對齊邏輯有 bug'
    # 交集後的長度比單一市場應該略少（兩邊都營業的日子）
    assert len(rs) > 100


# ────────────────────────────────────────────────────────────────
# 3.3 缺失資料處理
# ────────────────────────────────────────────────────────────────

def test_missing_data_consistency():
    """故意建立 5 個 NaN，驗證行為一致（丟棄 NaN 日）"""
    dates = pd.date_range('2024-01-01', periods=50, freq='B')
    stock = pd.Series(np.linspace(100, 110, 50), index=dates)
    index = pd.Series(np.linspace(50, 55, 50), index=dates)
    # 故意設 5 個 NaN
    stock.iloc[10:15] = np.nan

    rs = calculate_rs_line(stock, index)
    # 應該丟棄 5 個 NaN 日
    assert len(rs) == 45, f'NaN 處理錯誤：預期 45 日，實際 {len(rs)}'
    assert rs.notna().all(), '殘留 NaN'


def test_zero_price_skipped():
    """價格為 0 的日子應該被丟棄（避免除以 0）"""
    dates = pd.date_range('2024-01-01', periods=20, freq='B')
    stock = pd.Series(100.0, index=dates)
    index = pd.Series(50.0, index=dates)
    index.iloc[5] = 0  # 製造除以 0 的陷阱

    rs = calculate_rs_line(stock, index)
    assert len(rs) == 19  # 丟棄 1 日
    assert np.isfinite(rs).all(), 'RS 出現 inf'


# ────────────────────────────────────────────────────────────────
# 3.4 新上市股票（短歷史）
# ────────────────────────────────────────────────────────────────

def test_short_history_rddt(snapshot):
    """RDDT 2024-03-21 上市，到 2024-12-31 不到 252 個交易日。

    驗收：
    - compute_returns 對短歷史 → 較長期間 return = 0（不誤導）
    - calculate_rs_line 仍可計算（截至已有日期）
    """
    rddt = _extract_close(snapshot, 'RDDT')
    if rddt is None:
        pytest.skip('RDDT 不在快照中')
    assert len(rddt) < 252, \
        f'RDDT 不應有 ≥252 個交易日（實際 {len(rddt)}）'

    # compute_returns：長期間應該 = 0
    df = pd.DataFrame({'Close': rddt})
    rets = compute_returns(df, periods_days=(65, 130, 195, 252))
    # 13w (65d) 可能有值，52w (252d) 必然為 0
    assert rets['52w'] == 0, \
        f'短歷史 52w return 應為 0（保護機制），實際 {rets["52w"]}'


# ────────────────────────────────────────────────────────────────
# 3.5 停牌/缺資料的處理
# ────────────────────────────────────────────────────────────────

def test_long_suspension_simulated():
    """模擬 10 個連續交易日停牌（NaN），RS Line 應該無誤值"""
    dates = pd.date_range('2024-01-01', periods=60, freq='B')
    stock = pd.Series(100.0, index=dates) * (1.001 ** np.arange(60))
    index = pd.Series(50.0, index=dates) * (1.0005 ** np.arange(60))
    # 停牌 10 日
    stock.iloc[20:30] = np.nan

    rs = calculate_rs_line(stock, index)
    assert len(rs) == 50, f'停牌期應丟棄 10 日 → 50 日，實際 {len(rs)}'
    assert rs.notna().all()
    assert np.isfinite(rs).all()


# ────────────────────────────────────────────────────────────────
# 3.6 額外：RS 計算的時間不變性（同一日資料多次計算結果相同）
# ────────────────────────────────────────────────────────────────

def test_rs_idempotent(snapshot):
    """同一筆資料、同一個函式呼叫兩次 → 結果完全相同"""
    nvda = _extract_close(snapshot, 'NVDA')
    spx  = _extract_close(snapshot, '^GSPC')
    if nvda is None or spx is None:
        pytest.skip('資料不在快照中')

    rs1 = calculate_rs_line(nvda, spx)
    rs2 = calculate_rs_line(nvda, spx)
    pd.testing.assert_series_equal(rs1, rs2)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
