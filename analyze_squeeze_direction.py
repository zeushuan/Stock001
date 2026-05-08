"""盤整方向預測研究（v9.17）
==========================================================
驗證假設：BB Squeeze 結束時的突破方向是否可預測？

核心假設
---------
H1: Squeeze 前 EMA20 > EMA60（多排）→ 70%+ 機率向上突破
H2: Squeeze 前 EMA20 < EMA60（空排）→ 70%+ 機率向下突破
H3: 量價分析（吸籌/出貨）能輔助預測
H4: 相對強度（vs 大盤）能輔助預測

定義
-----
- BB Squeeze 開始：bandwidth 進入近 120 日的 ≤ 20%ile
- BB Squeeze 結束：bandwidth 突破 20%ile 進入 expansion
- 突破方向：Squeeze 結束後 5/10 天的 close vs Squeeze 結束日 close
  > +2% = 向上突破
  < -2% = 向下突破
  其他 = 無方向（pseudo-breakout）

統計
-----
對每個 squeeze event 記錄：
- 結束日的 EMA 排列（多/空/糾纏）
- Squeeze 期間的量比變化（吸籌 vs 出貨）
- Squeeze 期間 ADX 變化
- 突破後 N 天的方向 + 報酬

輸出
-----
- 各「Squeeze 前狀態」對應的「突破方向比例」表
- 各種輔助訊號（量、RS）的預測強度
- 最佳預測組合

執行
-----
  python analyze_squeeze_direction.py                # TW
  python analyze_squeeze_direction.py --market us
  python analyze_squeeze_direction.py --market both
"""
import sys, json, time, argparse
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor

import numpy as np
import pandas as pd

try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import data_loader as dl
from analyze_swing_dynamic_exit import get_universe, COST_ROUND_TRIP
from bb_signals import compute_bb

WORKERS = 12
START_DATE = '2020-01-01'

# Squeeze 定義
SQUEEZE_LOOKBACK = 120     # 計算 bandwidth 百分位數的窗口
SQUEEZE_PCTILE = 20        # ≤ 20%ile = squeeze
EXPANSION_PCTILE = 50      # > 50%ile = no longer squeeze

# 突破判定
DIRECTION_DAYS = 10        # 突破後 N 天判定方向
UP_THRESHOLD = 2.0         # +2% = 向上突破
DOWN_THRESHOLD = -2.0      # -2% = 向下突破


def find_squeeze_events(df):
    """找出一個 ticker 的所有 BB Squeeze 結束事件。

    回傳 list of dict，每個 dict 描述一次 squeeze event:
      {
        'squeeze_start_idx': 開始 i,
        'squeeze_end_idx': 結束 i,
        'duration': 持續天數,
        ...各種輔助資訊
      }
    """
    if len(df) < SQUEEZE_LOOKBACK + 30:
        return []

    c = df['Close'].values
    h = df['High'].values
    l = df['Low'].values
    v = df['Volume'].values

    bb = compute_bb(c)
    bw = bb['bandwidth']

    e20 = df['e20'].values
    e60 = df['e60'].values
    adx = df['adx'].values

    n = len(df)

    # 計算 bandwidth percentile（rolling 120 days）
    pctile = np.full(n, np.nan)
    for i in range(SQUEEZE_LOOKBACK, n):
        window = bw[i - SQUEEZE_LOOKBACK:i]
        valid = window[~np.isnan(window)]
        if len(valid) >= 60 and not np.isnan(bw[i]):
            pctile[i] = (np.sum(valid <= bw[i]) / len(valid)) * 100

    # 找 squeeze events：連續 ≤ 20%ile，然後跳到 > 50%ile
    events = []
    in_squeeze = False
    squeeze_start = None
    for i in range(SQUEEZE_LOOKBACK, n):
        p = pctile[i]
        if np.isnan(p): continue
        if not in_squeeze and p <= SQUEEZE_PCTILE:
            in_squeeze = True
            squeeze_start = i
        elif in_squeeze and p > EXPANSION_PCTILE:
            in_squeeze = False
            squeeze_end = i - 1
            duration = squeeze_end - squeeze_start + 1
            if duration < 5:  # 至少 5 天的 squeeze
                continue
            if squeeze_end + DIRECTION_DAYS >= n: break  # 不夠 forward 期間

            # 計算各項輔助訊號
            # ① EMA 排列（squeeze 結束時）
            if np.isnan(e20[squeeze_end]) or np.isnan(e60[squeeze_end]):
                continue
            if e20[squeeze_end] > e60[squeeze_end]:
                if e60[squeeze_end] > 0 and (e20[squeeze_end] - e60[squeeze_end]) / e60[squeeze_end] * 100 > 0.5:
                    ema_state = 'BULL'
                else:
                    ema_state = 'TANGLED'
            elif e20[squeeze_end] < e60[squeeze_end]:
                if e60[squeeze_end] > 0 and (e60[squeeze_end] - e20[squeeze_end]) / e60[squeeze_end] * 100 > 0.5:
                    ema_state = 'BEAR'
                else:
                    ema_state = 'TANGLED'
            else:
                ema_state = 'TANGLED'

            # ② Squeeze 期間量價分析（吸籌 vs 出貨）
            try:
                up_vol = []; down_vol = []
                for j in range(squeeze_start, squeeze_end + 1):
                    if j < 1: continue
                    if c[j] > c[j-1]:
                        up_vol.append(v[j])
                    elif c[j] < c[j-1]:
                        down_vol.append(v[j])
                avg_up = float(np.mean(up_vol)) if up_vol else 0
                avg_down = float(np.mean(down_vol)) if down_vol else 0
                if avg_up > 0 and avg_down > 0:
                    vol_ratio = avg_up / avg_down  # > 1 = 吸籌
                else:
                    vol_ratio = 1.0
                if vol_ratio > 1.3:
                    vol_signal = 'ACCUMULATION'   # 吸籌
                elif vol_ratio < 0.77:
                    vol_signal = 'DISTRIBUTION'   # 出貨
                else:
                    vol_signal = 'NEUTRAL'
            except Exception:
                vol_ratio = 1.0
                vol_signal = 'NEUTRAL'

            # ③ ADX 趨勢（squeeze 期間 ADX 是否上升 = 即將突破）
            adx_at_end = float(adx[squeeze_end]) if not np.isnan(adx[squeeze_end]) else 0
            adx_at_start = float(adx[squeeze_start]) if not np.isnan(adx[squeeze_start]) else 0
            adx_diff = adx_at_end - adx_at_start

            # ④ 突破方向（squeeze 結束後 N 天）
            close_end = float(c[squeeze_end])
            if close_end <= 0: continue
            close_after = float(c[squeeze_end + DIRECTION_DAYS])
            ret_pct = (close_after - close_end) / close_end * 100
            if ret_pct > UP_THRESHOLD:
                direction = 'UP'
            elif ret_pct < DOWN_THRESHOLD:
                direction = 'DOWN'
            else:
                direction = 'NEUTRAL'

            events.append({
                'squeeze_start': squeeze_start,
                'squeeze_end': squeeze_end,
                'duration': duration,
                'ema_state': ema_state,
                'vol_signal': vol_signal,
                'vol_ratio': round(vol_ratio, 2),
                'adx_diff': round(adx_diff, 1),
                'adx_at_end': round(adx_at_end, 1),
                'direction': direction,
                'ret_pct': round(ret_pct, 2),
                'close_end': round(close_end, 2),
            })

    return events


def gen_events_one(args):
    ticker, start_date = args
    out = []
    try:
        df = dl.load_from_cache(ticker)
        if df is None or len(df) < 280:
            return out
        if hasattr(df.index, 'tz') and df.index.tz is not None:
            df = df.copy()
            df.index = df.index.tz_localize(None)
        df = df[df.index >= pd.Timestamp(start_date)]
        if len(df) < 200:
            return out
        events = find_squeeze_events(df)
        for e in events:
            e['ticker'] = ticker
        return events
    except Exception:
        return out


def analyze_events(events_df, market_label):
    print('\n' + '=' * 110)
    print(f'📊 {market_label} BB Squeeze 突破方向分析 ({len(events_df)} 個事件)')
    print('=' * 110)

    # 整體分布
    direction_count = events_df['direction'].value_counts()
    print(f'\n總體突破方向分布:')
    for d, n in direction_count.items():
        pct = n / len(events_df) * 100
        print(f'  {d}: {n} ({pct:.1f}%)')

    # 按 EMA 排列分組
    print('\n📊 按 Squeeze 前 EMA 排列分組:')
    print(f'{"EMA":>10} {"n":>6} {"UP%":>7} {"DOWN%":>7} {"NEU%":>7} {"avg_ret":>9}')
    print('-' * 60)
    for ema in ['BULL', 'BEAR', 'TANGLED']:
        sub = events_df[events_df['ema_state'] == ema]
        if len(sub) == 0: continue
        up_pct = (sub['direction'] == 'UP').mean() * 100
        dn_pct = (sub['direction'] == 'DOWN').mean() * 100
        nu_pct = (sub['direction'] == 'NEUTRAL').mean() * 100
        avg_ret = sub['ret_pct'].mean()
        print(f'{ema:>10} {len(sub):>6} {up_pct:>6.1f}% {dn_pct:>6.1f}% {nu_pct:>6.1f}% {avg_ret:>+8.2f}%')

    # 按 量價訊號分組
    print('\n📊 按 Squeeze 期間量價訊號分組:')
    print(f'{"vol":>15} {"n":>6} {"UP%":>7} {"DOWN%":>7} {"avg_ret":>9}')
    print('-' * 65)
    for vol in ['ACCUMULATION', 'DISTRIBUTION', 'NEUTRAL']:
        sub = events_df[events_df['vol_signal'] == vol]
        if len(sub) == 0: continue
        up_pct = (sub['direction'] == 'UP').mean() * 100
        dn_pct = (sub['direction'] == 'DOWN').mean() * 100
        avg_ret = sub['ret_pct'].mean()
        print(f'{vol:>15} {len(sub):>6} {up_pct:>6.1f}% {dn_pct:>6.1f}% {avg_ret:>+8.2f}%')

    # 組合分組（EMA × vol）
    print('\n📊 EMA + 量價組合（top 6 by n）:')
    print(f'{"EMA":>10} {"vol":>15} {"n":>6} {"UP%":>7} {"DOWN%":>7} {"avg_ret":>9} {"strong?":>9}')
    print('-' * 75)
    for ema in ['BULL', 'BEAR', 'TANGLED']:
        for vol in ['ACCUMULATION', 'DISTRIBUTION', 'NEUTRAL']:
            sub = events_df[(events_df['ema_state'] == ema) & (events_df['vol_signal'] == vol)]
            if len(sub) < 10: continue
            up_pct = (sub['direction'] == 'UP').mean() * 100
            dn_pct = (sub['direction'] == 'DOWN').mean() * 100
            avg_ret = sub['ret_pct'].mean()
            strong = ('🔥 強看多' if up_pct >= 60 else
                      '❄️ 強看空' if dn_pct >= 60 else
                      '🌫️ 中性')
            print(f'{ema:>10} {vol:>15} {len(sub):>6} {up_pct:>6.1f}% {dn_pct:>6.1f}% {avg_ret:>+8.2f}% {strong:>9}')

    # 按 ADX 是否上升分組（squeeze 中 ADX 上升 = 即將突破？）
    print('\n📊 按 Squeeze 期間 ADX 變化分組:')
    print(f'{"ADX":>15} {"n":>6} {"UP%":>7} {"DOWN%":>7} {"avg_ret":>9}')
    print('-' * 65)
    events_df['adx_label'] = events_df['adx_diff'].apply(
        lambda x: '上升 ≥+5' if x >= 5 else '下降 ≥-5' if x <= -5 else '中性')
    for label in ['上升 ≥+5', '中性', '下降 ≥-5']:
        sub = events_df[events_df['adx_label'] == label]
        if len(sub) == 0: continue
        up_pct = (sub['direction'] == 'UP').mean() * 100
        dn_pct = (sub['direction'] == 'DOWN').mean() * 100
        avg_ret = sub['ret_pct'].mean()
        print(f'{label:>15} {len(sub):>6} {up_pct:>6.1f}% {dn_pct:>6.1f}% {avg_ret:>+8.2f}%')

    # Best combo
    print('\n🏆 最佳預測組合（n ≥ 30 且 UP%+DOWN% 不對稱）:')
    combos = []
    for ema in ['BULL', 'BEAR', 'TANGLED']:
        for vol in ['ACCUMULATION', 'DISTRIBUTION', 'NEUTRAL']:
            for adxl in ['上升 ≥+5', '中性', '下降 ≥-5']:
                sub = events_df[(events_df['ema_state'] == ema) &
                                (events_df['vol_signal'] == vol) &
                                (events_df['adx_label'] == adxl)]
                if len(sub) < 30: continue
                up_pct = (sub['direction'] == 'UP').mean() * 100
                dn_pct = (sub['direction'] == 'DOWN').mean() * 100
                avg_ret = sub['ret_pct'].mean()
                bias = up_pct - dn_pct
                combos.append((ema, vol, adxl, len(sub), up_pct, dn_pct, avg_ret, bias))
    combos.sort(key=lambda x: -abs(x[7]))
    print(f'{"EMA":>10} {"vol":>15} {"ADX":>15} {"n":>6} {"UP%":>7} {"DOWN%":>7} {"avg_ret":>9} {"bias":>7}')
    for c in combos[:8]:
        ema, vol, adxl, n, up_pct, dn_pct, avg_ret, bias = c
        print(f'{ema:>10} {vol:>15} {adxl:>15} {n:>6} {up_pct:>6.1f}% {dn_pct:>6.1f}% {avg_ret:>+8.2f}% {bias:>+6.1f}')


def run(market='tw'):
    universe = get_universe(market)
    flag = '🇹🇼' if market == 'tw' else '🇺🇸'
    print(f'\n{flag} BB Squeeze 突破方向研究')
    print(f'  Universe: {len(universe)} 檔 / 期間 {START_DATE}-')
    print(f'  Squeeze: bandwidth ≤ {SQUEEZE_PCTILE}%ile / Expansion: > {EXPANSION_PCTILE}%ile')
    print(f'  方向判定：squeeze end + {DIRECTION_DAYS} 天')
    print()

    print(f'📊 跑訊號（{WORKERS} workers）...')
    t0 = time.time()
    args = [(t, START_DATE) for t in universe]
    all_events = []
    with ProcessPoolExecutor(max_workers=WORKERS) as ex:
        for events in ex.map(gen_events_one, args, chunksize=80):
            all_events.extend(events)
    print(f'  完成 {time.time()-t0:.1f}s，共 {len(all_events)} 個 squeeze events')

    if not all_events:
        print('❌ 沒有 events')
        return

    df = pd.DataFrame(all_events)
    analyze_events(df, market.upper())

    # 寫 JSON
    out = f'analyze_squeeze_direction_{market}.json'
    summary = {
        'market': market,
        'universe_size': len(universe),
        'n_events': len(all_events),
        'overall_direction': df['direction'].value_counts().to_dict(),
        'by_ema_state': df.groupby('ema_state')['direction'].value_counts(normalize=True).to_dict(),
        'by_vol_signal': df.groupby('vol_signal')['direction'].value_counts(normalize=True).to_dict(),
    }
    summary['by_ema_state'] = {f'{k[0]}_{k[1]}': v for k, v in summary['by_ema_state'].items()}
    summary['by_vol_signal'] = {f'{k[0]}_{k[1]}': v for k, v in summary['by_vol_signal'].items()}
    with open(out, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    print(f'\n✅ 寫入 {out}')


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--market', type=str, default='tw',
                   choices=['tw', 'us', 'both'])
    args = p.parse_args()
    markets = ['tw', 'us'] if args.market == 'both' else [args.market]
    for m in markets:
        run(m)


if __name__ == '__main__':
    main()
