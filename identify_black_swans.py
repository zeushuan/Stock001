"""識別歷史黑天鵝事件
============================
觸發條件（多重 OR）：
  1. VIX > 35（恐慌爆表）
  2. TWII 單日 < -3%
  3. SP500 單日 < -3%
  4. SOX 單日 < -4%

對每個觸發日：
  - 標記為「黑天鵝起點」
  - 30 個交易日危險窗
  - 結束條件：VIX < 25 連續 5 日 OR 30 日窗結束
"""
import sys
from pathlib import Path
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import pandas as pd
import numpy as np
import json


def load(ticker):
    p = Path('data_cache') / f'{ticker}.parquet'
    if not p.exists(): return None
    return pd.read_parquet(p)


def main():
    vix  = load('^VIX')
    twii = load('^TWII')
    spx  = load('^GSPC')
    sox  = load('^SOX')

    # 對齊到日期
    df = pd.DataFrame({
        'VIX_close':  vix['Close'],
        'TWII_close': twii['Close'].reindex(vix.index, method='ffill'),
        'SPX_close':  spx['Close'].reindex(vix.index, method='ffill'),
        'SOX_close':  sox['Close'].reindex(vix.index, method='ffill'),
    })
    # 計算單日報酬
    df['TWII_ret'] = df['TWII_close'].pct_change() * 100
    df['SPX_ret']  = df['SPX_close'].pct_change() * 100
    df['SOX_ret']  = df['SOX_close'].pct_change() * 100

    # 觸發條件
    df['BS_trigger'] = (
        (df['VIX_close'] > 35) |
        (df['TWII_ret'] < -3) |
        (df['SPX_ret']  < -3) |
        (df['SOX_ret']  < -4)
    )
    df['VIX_calm'] = df['VIX_close'] < 25  # 恢復條件參考

    # 找連續 trigger 區段（合併 30 日內的多個 trigger）
    triggers = df[df['BS_trigger']].copy()
    print(f"\n📊 黑天鵝觸發日總數：{len(triggers)} 日（從 2019-03 起算）\n")
    print(f"觸發來源拆解：")
    print(f"  VIX > 35:        {(df['VIX_close'] > 35).sum():>4} 日")
    print(f"  TWII < -3%:      {(df['TWII_ret'] < -3).sum():>4} 日")
    print(f"  SPX < -3%:       {(df['SPX_ret']  < -3).sum():>4} 日")
    print(f"  SOX < -4%:       {(df['SOX_ret']  < -4).sum():>4} 日")
    print()

    # 識別事件群（相鄰 ≤ 10 個日合併為一個事件，縮緊以避開 2022 全年合併）
    events = []
    cur_event_start = None
    cur_event_end = None
    cur_max_vix = 0
    cur_min_twii = 0
    last_idx = None

    for ts, row in triggers.iterrows():
        if last_idx is None:
            cur_event_start = ts
            cur_event_end = ts
            cur_max_vix = row['VIX_close']
            cur_min_twii = row['TWII_ret']
            last_idx = ts
        else:
            days_gap = (ts - last_idx).days
            if days_gap <= 10:  # 同一事件
                cur_event_end = ts
                cur_max_vix = max(cur_max_vix, row['VIX_close'])
                cur_min_twii = min(cur_min_twii, row['TWII_ret'])
            else:
                # 結算前一事件
                events.append({
                    'start': cur_event_start,
                    'end': cur_event_end,
                    'days': (cur_event_end - cur_event_start).days,
                    'max_vix': cur_max_vix,
                    'min_twii_ret': cur_min_twii,
                })
                cur_event_start = ts
                cur_event_end = ts
                cur_max_vix = row['VIX_close']
                cur_min_twii = row['TWII_ret']
            last_idx = ts

    # 收尾
    if cur_event_start is not None:
        events.append({
            'start': cur_event_start,
            'end': cur_event_end,
            'days': (cur_event_end - cur_event_start).days,
            'max_vix': cur_max_vix,
            'min_twii_ret': cur_min_twii,
        })

    print(f"📅 識別出 {len(events)} 個黑天鵝事件：\n")
    print(f"{'#':<3} {'起':<12} {'迄':<12} {'天':>4} {'max VIX':>9} {'最低 TWII%':>10}  事件")
    print("-" * 90)
    NAME_HINT = {
        '2020-02': 'COVID 崩盤',
        '2020-03': 'COVID 崩盤',
        '2021-01': '科技股震盪',
        '2022-01': '聯準會升息',
        '2022-02': 'Ukraine 戰爭',
        '2022-04': '通膨高峰',
        '2022-05': '通膨高峰',
        '2022-06': '通膨高峰',
        '2022-09': '殖利率倒掛',
        '2023-03': 'SVB 銀行危機',
        '2024-04': '中東衝突',
        '2024-08': '日圓套利平倉',
        '2025-02': 'DeepSeek 衝擊',
        '2025-04': 'Trump 關稅',
    }
    for i, e in enumerate(events, 1):
        ym = e['start'].strftime('%Y-%m')
        hint = NAME_HINT.get(ym, '—')
        print(f"{i:<3} {e['start'].strftime('%Y-%m-%d'):<12} "
              f"{e['end'].strftime('%Y-%m-%d'):<12} "
              f"{e['days']:>4} {e['max_vix']:>9.1f} {e['min_twii_ret']:>+9.2f}  {hint}")

    # 存檔給後續變體使用
    out = []
    for e in events:
        out.append({
            'start': e['start'].strftime('%Y-%m-%d'),
            'end':   e['end'].strftime('%Y-%m-%d'),
            'max_vix': float(e['max_vix']),
            'min_twii_ret': float(e['min_twii_ret']),
        })
    with open('black_swans.json', 'w', encoding='utf-8') as f:
        json.dump(out, f, indent=2, ensure_ascii=False)
    print(f"\n✅ 寫入 black_swans.json ({len(out)} 個事件)")

    # 計算「危險窗」涵蓋天數比例
    # 危險窗 = trigger 起始 + 7 個交易日（縮短，避免錯過 trigger 後反彈）
    # 額外條件：VIX > 28 才繼續算危險，回到 < 25 立即解除
    danger_days = set()
    for e in events:
        s = e['start']
        en = e['start'] + pd.Timedelta(days=10)  # 約 7 個交易日
        for d in pd.bdate_range(s, en):
            # 檢查當天 VIX 是否仍 > 25
            if d in df.index:
                if df.loc[d, 'VIX_close'] > 25:
                    danger_days.add(d.date())
            else:
                danger_days.add(d.date())  # 沒資料保守加入
    total_days = pd.bdate_range(df.index.min(), df.index.max()).size
    coverage = len(danger_days) / total_days * 100
    print(f"\n⚠️ 危險窗 (trigger 起 21 日內) 占歷史 {coverage:.1f}% 的交易日")
    print(f"   ({len(danger_days)} / {total_days} 個交易日)")

    # 🆕 POST10：trigger 結束日 +1~+10 BD（EDA 證實是「假反彈」最差期）
    post10_days = set()
    # 🆕 POST30：trigger 結束日 +11~+30 BD（EDA 證實是 T4 反彈黃金期）
    post30_days = set()
    for e in events:
        end_ts = pd.Timestamp(e['end'])
        post10_range = pd.bdate_range(end_ts + pd.tseries.offsets.BDay(1),
                                       end_ts + pd.tseries.offsets.BDay(10))
        for d in post10_range:
            post10_days.add(d.date())
        post30_range = pd.bdate_range(end_ts + pd.tseries.offsets.BDay(11),
                                       end_ts + pd.tseries.offsets.BDay(30))
        for d in post30_range:
            post30_days.add(d.date())
    print(f"\n🆕 POST10 (trigger end +1~+10 BD): {len(post10_days)} 日 "
          f"({len(post10_days)/total_days*100:.1f}%)")
    print(f"🆕 POST30 (trigger end +11~+30 BD): {len(post30_days)} 日 "
          f"({len(post30_days)/total_days*100:.1f}%)")

    # 寫進 JSON 給 variant 用
    out2 = {
        'events': out,
        'danger_dates': sorted(d.isoformat() for d in danger_days),
        'post10_dates': sorted(d.isoformat() for d in post10_days),
        'post30_dates': sorted(d.isoformat() for d in post30_days),
    }
    with open('black_swans.json', 'w', encoding='utf-8') as f:
        json.dump(out2, f, indent=2, ensure_ascii=False)
    print(f"\n✅ 寫入 black_swans.json (events={len(out)}, danger={len(danger_days)}, "
          f"post10={len(post10_days)}, post30={len(post30_days)})")


if __name__ == '__main__':
    main()
