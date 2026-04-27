"""
K 線型態偵測（純偵測函式，供 tv_app.py UI 顯示用）

15 個型態，依方向分類：
  反轉多頭（5）：HAMMER 鎚子, INV_HAMMER 倒鎚, BULL_ENGULF 多頭吞噬,
                PIERCING 旭日東昇, MORNING_STAR 啟明星
  反轉空頭（5）：HANGING_MAN 吊人, SHOOTING_STAR 流星, BEAR_ENGULF 空頭吞噬,
                DARK_CLOUD 烏雲蓋頂, EVENING_STAR 黃昏星
  持續（4）：THREE_SOLDIERS 紅三兵, THREE_CROWS 三隻烏鴉,
            MARUBOZU_BULL 大陽線, MARUBOZU_BEAR 大陰線
  中性（1）：DOJI 十字星

每個函式接收 OHLC + ATR arrays + 索引 i，回傳 True/False。
"""
import numpy as np


def _body(o, c): return abs(c - o)
def _upper(o, h, c): return h - max(o, c)
def _lower(o, l, c): return min(o, c) - l
def _bull(o, c): return c > o
def _bear(o, c): return c < o


# ─── 單 K 棒型態 ─────────────────────────────────────────────────
def detect_hammer(o, h, l, c, atr, i):
    if i < 5: return False
    body = _body(o[i], c[i])
    if body < atr[i] * 0.1: return False
    if _lower(o[i], l[i], c[i]) < body * 2: return False
    if _upper(o[i], h[i], c[i]) > body * 0.5: return False
    if c[i-1] >= c[i-5]: return False
    return True

def detect_inv_hammer(o, h, l, c, atr, i):
    if i < 5: return False
    body = _body(o[i], c[i])
    if body < atr[i] * 0.1: return False
    if _upper(o[i], h[i], c[i]) < body * 2: return False
    if _lower(o[i], l[i], c[i]) > body * 0.5: return False
    if c[i-1] >= c[i-5]: return False
    return True

def detect_hanging_man(o, h, l, c, atr, i):
    if i < 5: return False
    body = _body(o[i], c[i])
    if body < atr[i] * 0.1: return False
    if _lower(o[i], l[i], c[i]) < body * 2: return False
    if _upper(o[i], h[i], c[i]) > body * 0.5: return False
    if c[i-1] <= c[i-5]: return False
    return True

def detect_shooting_star(o, h, l, c, atr, i):
    if i < 5: return False
    body = _body(o[i], c[i])
    if body < atr[i] * 0.1: return False
    if _upper(o[i], h[i], c[i]) < body * 2: return False
    if _lower(o[i], l[i], c[i]) > body * 0.5: return False
    if c[i-1] <= c[i-5]: return False
    return True

def detect_doji(o, h, l, c, atr, i):
    if i < 1: return False
    return _body(o[i], c[i]) < atr[i] * 0.1

def detect_marubozu_bull(o, h, l, c, atr, i):
    if i < 1: return False
    body = _body(o[i], c[i])
    if not _bull(o[i], c[i]): return False
    if body < atr[i] * 1.0: return False
    if _upper(o[i], h[i], c[i]) > body * 0.05: return False
    if _lower(o[i], l[i], c[i]) > body * 0.05: return False
    return True

def detect_marubozu_bear(o, h, l, c, atr, i):
    if i < 1: return False
    body = _body(o[i], c[i])
    if not _bear(o[i], c[i]): return False
    if body < atr[i] * 1.0: return False
    if _upper(o[i], h[i], c[i]) > body * 0.05: return False
    if _lower(o[i], l[i], c[i]) > body * 0.05: return False
    return True


# ─── 兩 K 棒型態 ─────────────────────────────────────────────────
def detect_bull_engulf(o, h, l, c, atr, i):
    if i < 1: return False
    if not _bear(o[i-1], c[i-1]): return False
    if not _bull(o[i], c[i]): return False
    if o[i] > c[i-1]: return False
    if c[i] < o[i-1]: return False
    return True

def detect_bear_engulf(o, h, l, c, atr, i):
    if i < 1: return False
    if not _bull(o[i-1], c[i-1]): return False
    if not _bear(o[i], c[i]): return False
    if o[i] < c[i-1]: return False
    if c[i] > o[i-1]: return False
    return True

def detect_piercing(o, h, l, c, atr, i):
    if i < 1: return False
    if not _bear(o[i-1], c[i-1]): return False
    if not _bull(o[i], c[i]): return False
    prev_mid = (o[i-1] + c[i-1]) / 2
    if o[i] >= l[i-1]: return False
    if c[i] <= prev_mid: return False
    if c[i] >= o[i-1]: return False
    return True

def detect_dark_cloud(o, h, l, c, atr, i):
    if i < 1: return False
    if not _bull(o[i-1], c[i-1]): return False
    if not _bear(o[i], c[i]): return False
    prev_mid = (o[i-1] + c[i-1]) / 2
    if o[i] <= h[i-1]: return False
    if c[i] >= prev_mid: return False
    if c[i] <= o[i-1]: return False
    return True


# ─── 三 K 棒型態 ─────────────────────────────────────────────────
def detect_morning_star(o, h, l, c, atr, i):
    if i < 2: return False
    if not _bear(o[i-2], c[i-2]): return False
    body0 = _body(o[i-2], c[i-2])
    if _body(o[i-1], c[i-1]) > body0 * 0.5: return False
    if not _bull(o[i], c[i]): return False
    if _body(o[i], c[i]) < body0 * 0.5: return False
    prev_mid = (o[i-2] + c[i-2]) / 2
    if c[i] <= prev_mid: return False
    return True

def detect_evening_star(o, h, l, c, atr, i):
    if i < 2: return False
    if not _bull(o[i-2], c[i-2]): return False
    body0 = _body(o[i-2], c[i-2])
    if _body(o[i-1], c[i-1]) > body0 * 0.5: return False
    if not _bear(o[i], c[i]): return False
    if _body(o[i], c[i]) < body0 * 0.5: return False
    prev_mid = (o[i-2] + c[i-2]) / 2
    if c[i] >= prev_mid: return False
    return True

def detect_three_soldiers(o, h, l, c, atr, i):
    if i < 2: return False
    for k in [i-2, i-1, i]:
        if not _bull(o[k], c[k]): return False
    if not (c[i-2] < c[i-1] < c[i]): return False
    if not (o[i-1] > o[i-2] and o[i-1] < c[i-2]): return False
    if not (o[i] > o[i-1] and o[i] < c[i-1]): return False
    return True

def detect_three_crows(o, h, l, c, atr, i):
    if i < 2: return False
    for k in [i-2, i-1, i]:
        if not _bear(o[k], c[k]): return False
    if not (c[i-2] > c[i-1] > c[i]): return False
    if not (o[i-1] < o[i-2] and o[i-1] > c[i-2]): return False
    if not (o[i] < o[i-1] and o[i] > c[i-1]): return False
    return True


# ─── 註冊表 ──────────────────────────────────────────────────────
PATTERNS = {
    'HAMMER':         (detect_hammer,         'bull',    '🔨 鎚子線',      '跌勢末端反轉訊號'),
    'INV_HAMMER':     (detect_inv_hammer,     'bull',    '🔨 倒鎚線',      '跌勢末端反轉訊號（5d edge +0.35）'),
    'BULL_ENGULF':    (detect_bull_engulf,    'bull',    '🟢 多頭吞噬',    '今紅 K 完全吞噬昨黑 K'),
    'PIERCING':       (detect_piercing,       'bull',    '🌅 旭日東昇',    '黑 K 後紅 K 收破中點'),
    'MORNING_STAR':   (detect_morning_star,   'bull',    '⭐ 啟明星',      '三星反轉，跌勢末端'),
    'THREE_SOLDIERS': (detect_three_soldiers, 'bull',    '🪖 紅三兵',      '連 3 紅 K 創高'),
    'MARUBOZU_BULL':  (detect_marubozu_bull,  'bull',    '🟢 大陽線',      '無上下影線的大紅 K'),
    'HANGING_MAN':    (detect_hanging_man,    'bear',    '⚠️ 吊人線',      '漲勢末端反轉訊號'),
    'SHOOTING_STAR':  (detect_shooting_star,  'bear',    '☄️ 流星線',      '漲勢末端反轉訊號'),
    'BEAR_ENGULF':    (detect_bear_engulf,    'bear',    '🔴 空頭吞噬',    '今黑 K 完全吞噬昨紅 K'),
    'DARK_CLOUD':     (detect_dark_cloud,     'bear',    '🌧 烏雲蓋頂',    '紅 K 後黑 K 收破中點'),
    'EVENING_STAR':   (detect_evening_star,   'bear',    '🌙 黃昏星',      '三星反轉，漲勢末端'),
    'THREE_CROWS':    (detect_three_crows,    'bear',    '🕊 三隻烏鴉',    '連 3 黑 K 創低'),
    'MARUBOZU_BEAR':  (detect_marubozu_bear,  'bear',    '🔴 大陰線',      '無上下影線的大黑 K'),
    'DOJI':           (detect_doji,           'neutral', '✨ 十字星',      '猶豫不決，趨勢可能反轉'),
}


def detect_all(df) -> dict:
    """掃描整個 DataFrame 偵測所有型態。
    回傳 {pattern_name: [trigger_indices]}
    """
    if df is None or len(df) < 6: return {n: [] for n in PATTERNS}
    o = df['Open'].values
    h = df['High'].values
    l = df['Low'].values
    c = df['Close'].values
    atr = df['atr'].values if 'atr' in df.columns else np.full(len(df), np.nan)
    n = len(c)
    out = {name: [] for name in PATTERNS}
    for name, (fn, _, _, _) in PATTERNS.items():
        for i in range(2, n):
            if any(np.isnan([o[i], h[i], l[i], c[i], atr[i]])): continue
            try:
                if fn(o, h, l, c, atr, i):
                    out[name].append(i)
            except Exception:
                continue
    return out


def detect_recent(df, lookback: int = 5) -> list:
    """偵測最近 N 個交易日內出現的所有型態。
    回傳 [(date, name_zh, side, note, days_ago), ...]，依 days_ago 升冪。
    """
    if df is None or len(df) < 6:
        return []
    o = df['Open'].values
    h = df['High'].values
    l = df['Low'].values
    c = df['Close'].values
    atr = df['atr'].values if 'atr' in df.columns else np.full(len(df), np.nan)
    n = len(c)
    if 'atr' not in df.columns:
        # 簡易 ATR 計算
        import pandas as pd
        tr = pd.concat([
            (df['High'] - df['Low']).abs(),
            (df['High'] - df['Close'].shift()).abs(),
            (df['Low'] - df['Close'].shift()).abs(),
        ], axis=1).max(axis=1)
        atr = tr.rolling(14).mean().values

    out = []
    start = max(2, n - lookback)
    for i in range(start, n):
        if any(np.isnan([o[i], h[i], l[i], c[i], atr[i]])):
            continue
        for name, (fn, side, name_zh, note) in PATTERNS.items():
            try:
                if fn(o, h, l, c, atr, i):
                    days_ago = n - 1 - i
                    out.append({
                        'date': df.index[i].strftime('%Y-%m-%d'),
                        'name': name,
                        'name_zh': name_zh,
                        'side': side,
                        'note': note,
                        'days_ago': days_ago,
                    })
            except Exception:
                continue
    # 依 days_ago 升冪（最近的優先）
    out.sort(key=lambda x: x['days_ago'])
    return out
