"""資料源連線測試腳本（Alpaca + Polygon）
========================================

填好 .env 後執行：
  python test_alpaca.py
"""
import sys, io, os
try:
    if sys.stdout.encoding and sys.stdout.encoding.lower() != 'utf-8':
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
except Exception:
    pass

from intraday.data import (
    _has_alpaca, _fetch_alpaca, get_intraday,
    _has_polygon, _fetch_polygon,
    ALPACA_API_KEY, ALPACA_API_SECRET, ALPACA_PAPER,
    POLYGON_API_KEY,
)

print('═' * 60)
print('  Alpaca 連線測試')
print('═' * 60)

# 1. 配置檢查
print(f'\n[1] 環境變數:')
print(f'  ALPACA_API_KEY:    {"✅ 已設" if ALPACA_API_KEY else "❌ 未設"} '
      f'({ALPACA_API_KEY[:8]}...{ALPACA_API_KEY[-4:] if ALPACA_API_KEY else ""})')
print(f'  ALPACA_API_SECRET: {"✅ 已設" if ALPACA_API_SECRET else "❌ 未設"}')
print(f'  ALPACA_PAPER:      {ALPACA_PAPER} (paper = IEX feed)')
print(f'  _has_alpaca():     {_has_alpaca()}')

if not _has_alpaca():
    print('\n❌ 請先在 .env 設定 ALPACA_API_KEY 和 ALPACA_API_SECRET')
    sys.exit(1)

# 2. 抓 NVDA 1m 測試
print(f'\n[2] 抓 NVDA 5m 資料測試:')
df = _fetch_alpaca('NVDA', '5m')
if df is None or len(df) == 0:
    print('  ❌ Alpaca 沒回傳資料')
    sys.exit(1)
print(f'  ✅ 抓到 {len(df)} bars')
print(f'  最早: {df.index[0]}')
print(f'  最新: {df.index[-1]}')
print(f'  欄位: {list(df.columns)}')
print(f'  最後 3 bars:')
print(df.tail(3).to_string())

# 3. 檢查是否含夜盤
print(f'\n[3] 夜盤 bar 檢查 (pre-market 04:00-09:30 / after-hours 16:00-20:00 ET):')
df['hour_utc'] = df.index.hour
# ET 時間 = UTC - 4 (summer DST) 或 -5 (winter)
# pre-market: ET 4-9:30 = UTC 8-13:30 (summer)
# after-hours: ET 16-20 = UTC 20-24 (summer)
pre_bars = df[(df['hour_utc'] >= 8) & (df['hour_utc'] < 13)]
post_bars = df[(df['hour_utc'] >= 20)]
print(f'  Pre-market bars (~UTC 8-13): {len(pre_bars)}')
print(f'  After-hours bars (~UTC 20-24): {len(post_bars)}')

# 4. Polygon 測試
print(f'\n[4] Polygon.io 設定:')
print(f'  POLYGON_API_KEY: {"✅ 已設" if POLYGON_API_KEY else "❌ 未設"}'
      f'{f" ({POLYGON_API_KEY[:8]}...)" if POLYGON_API_KEY else ""}')
print(f'  _has_polygon():  {_has_polygon()}')

if _has_polygon():
    print(f'\n[5] Polygon 抓 SOXS 1m (含 overnight):')
    df_pg = _fetch_polygon('SOXS', '1m')
    if df_pg is None or len(df_pg) == 0:
        print('  ❌ Polygon 沒回資料（可能 free tier 限制 / API key 無效）')
    else:
        print(f'  ✅ 抓到 {len(df_pg)} bars')
        print(f'  最早: {df_pg.index[0]}')
        print(f'  最新: {df_pg.index[-1]}')
        print(f'  最新 5 bars:')
        print(df_pg.tail(5).to_string())
else:
    print(f'\n[5] Polygon 跳過（key 未設）')

# 6. get_intraday 完整三路合併
print(f'\n[6] 透過 get_intraday() 完整流程（三路智能合併）:')
df2 = get_intraday('SOXS', '5m', market='us', refresh=True)
if df2 is not None:
    print(f'  ✅ 拿到 {len(df2)} bars')
    print(f'  最新 bar: {df2.index[-1]} Close ${df2["Close"].iloc[-1]:.4f}')
else:
    print(f'  ❌ get_intraday 失敗')

print('\n✅ 資料源整合測試完成！')
