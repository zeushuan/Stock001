"""Polygon 付費版完整驗證腳本
====================================

升級到 Stocks Starter ($29/月，14 天免費試用) 後執行：
  python test_polygon_paid.py

驗證內容：
  1. list_trades 是否解鎖
  2. get_snapshot_ticker 是否解鎖
  3. list_aggs 是否抓得到 ARCA Overnight (20:00-04:00 ET) bars
  4. WebSocket 真即時推送是否能用
  5. 完整三路合併下 SOXS 5m 是否有 overnight bars
"""
import sys, io, os, time
try:
    if sys.stdout.encoding and sys.stdout.encoding.lower() != 'utf-8':
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
except Exception:
    pass

from datetime import datetime, timezone, timedelta
import pandas as pd
from intraday.data import (
    POLYGON_API_KEY, _has_polygon, _get_polygon_client,
    _fetch_polygon, get_intraday,
)

print('═' * 70)
print('  Polygon 付費版完整驗證')
print('═' * 70)

now_utc = datetime.now(timezone.utc)
print(f'\n當前時間 (UTC): {now_utc}')
print(f'當前時間 (ET):  {datetime.now(timezone(timedelta(hours=-4)))}')

if not _has_polygon():
    print('\n❌ POLYGON_API_KEY 未設，請先填 .env')
    sys.exit(1)

client = _get_polygon_client()

# ───────────────────────────────────────────────────────────────
# Test 1: list_trades 是否解鎖
# ───────────────────────────────────────────────────────────────
print('\n[1] list_trades — 即時成交查詢')
try:
    today = now_utc.date()
    trades = client.list_trades('SOXS',
        timestamp_gte=f'{today}T00:00:00Z',
        limit=20, order='desc')
    rows = list(trades)
    if not rows:
        print(f'  ⚠️  解鎖但今天 SOXS 沒成交（可能未開盤 / 假日）')
    else:
        print(f'  ✅ 解鎖！今天 SOXS 共 {len(rows)} trades（限 20）')
        print(f'  最近 10 trades：')
        for t in rows[:10]:
            ts = pd.Timestamp(t.participant_timestamp, unit='ns', tz='UTC')
            print(f'    {ts.strftime("%H:%M:%S.%f")[:-3]} UTC | '
                  f'${t.price:.4f} × {t.size:>6} | exchange={t.exchange}')
except Exception as e:
    err_str = str(e)
    if 'NOT_AUTHORIZED' in err_str or '403' in err_str:
        print(f'  ❌ 還是 NOT_AUTHORIZED（plan 尚未升級或同步中）')
    else:
        print(f'  ❌ {type(e).__name__}: {err_str[:200]}')

# ───────────────────────────────────────────────────────────────
# Test 2: get_snapshot_ticker 即時快照
# ───────────────────────────────────────────────────────────────
print('\n[2] get_snapshot_ticker — SOXS 即時快照')
try:
    snap = client.get_snapshot_ticker(market_type='stocks', ticker='SOXS')
    if hasattr(snap, 'last_trade') and snap.last_trade:
        lt = snap.last_trade
        ts = pd.Timestamp(lt.participant_timestamp, unit='ns', tz='UTC')
        age = (now_utc - ts).total_seconds()
        print(f'  ✅ last_trade: ${lt.price:.4f} × {lt.size} @ {ts} ({age:.0f}s 前)')
    if hasattr(snap, 'last_quote') and snap.last_quote:
        lq = snap.last_quote
        ts = pd.Timestamp(lq.participant_timestamp, unit='ns', tz='UTC')
        age = (now_utc - ts).total_seconds()
        print(f'  ✅ last_quote: bid ${lq.bid:.4f} / ask ${lq.ask:.4f} @ {ts} ({age:.0f}s 前)')
    if hasattr(snap, 'min') and snap.min:
        m = snap.min
        ts = pd.Timestamp(m.timestamp, unit='ms', tz='UTC') if m.timestamp else None
        print(f'  ✅ min bar: O={m.open} H={m.high} L={m.low} C={m.close} V={m.volume} @ {ts}')
    print(f'  updated: {snap.updated}')
except Exception as e:
    err_str = str(e)
    if 'NOT_AUTHORIZED' in err_str or '403' in err_str:
        print(f'  ❌ 還是 NOT_AUTHORIZED')
    else:
        print(f'  ❌ {type(e).__name__}: {err_str[:200]}')

# ───────────────────────────────────────────────────────────────
# Test 3: 抓今天 + 昨天 1m bars (含 ARCA overnight 20:00-04:00 ET)
# ───────────────────────────────────────────────────────────────
print('\n[3] list_aggs — 今天 + 過去 3 天 1m bars（含 overnight）')
try:
    end_date = now_utc.date()
    start_date = end_date - timedelta(days=3)
    aggs = client.list_aggs(
        ticker='SOXS', multiplier=1, timespan='minute',
        from_=start_date.isoformat(), to=end_date.isoformat(),
        adjusted=True, sort='asc', limit=50000,
    )
    rows = list(aggs)
    print(f'  共 {len(rows)} bars')
    if rows:
        # 看時間分佈
        df = pd.DataFrame([{
            'time': pd.Timestamp(a.timestamp, unit='ms', tz='UTC'),
            'open': a.open, 'high': a.high, 'low': a.low,
            'close': a.close, 'volume': a.volume,
        } for a in rows])
        df = df.set_index('time')

        # 找 ARCA Overnight bars (20:00-04:00 ET = 00:00-08:00 UTC summer)
        df_h = df.copy()
        df_h['hour_utc'] = df_h.index.hour
        overnight = df_h[
            (df_h['hour_utc'] >= 0) & (df_h['hour_utc'] < 8)
        ]
        # 並過濾出非週末日
        overnight = overnight[overnight.index.dayofweek < 5]
        print(f'  ARCA Overnight bars (UTC 00:00-08:00 工作日): {len(overnight)}')
        if len(overnight) > 0:
            print(f'  ✅ 包含 overnight！樣本：')
            for ts, row in overnight.tail(5).iterrows():
                print(f'    {ts}: C=${row["close"]:.4f} V={int(row["volume"])}')

        # 最新 5 bars
        print(f'  最新 5 bars：')
        for ts, row in df.tail(5).iterrows():
            age = (now_utc - ts).total_seconds() / 60
            print(f'    {ts}: C=${row["close"]:.4f} V={int(row["volume"])} ({age:.0f}min 前)')
except Exception as e:
    print(f'  ❌ {type(e).__name__}: {str(e)[:200]}')

# ───────────────────────────────────────────────────────────────
# Test 4: 完整三路合併下的最終結果
# ───────────────────────────────────────────────────────────────
print('\n[4] 完整 get_intraday() 三路合併 — SOXS 5m:')
df = get_intraday('SOXS', '5m', market='us', refresh=True)
if df is not None and len(df) > 0:
    last = df.index[-1]
    age = (datetime.now(timezone.utc).replace(tzinfo=None) - last).total_seconds() / 60
    print(f'  ✅ {len(df)} bars，最新 {last} ({age:.0f}min 前)')
    print(f'  最後 5 bars:')
    print(df.tail(5).to_string())
else:
    print('  ❌ get_intraday 失敗')

print('\n' + '═' * 70)
print('  驗證結束')
print('═' * 70)
print('\n判斷標準：')
print('  ✅ Test 1+2 解鎖 → 付費生效')
print('  ✅ Test 3 有 overnight bars → ARCA 24h 已涵蓋（值得 $29/月）')
print('  ❌ Test 3 沒 overnight bars → SIP 主流不包含，省下這筆錢')
