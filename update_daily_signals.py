"""每日預先計算 TOP 200 訊號 → 存 JSON
==========================================
Streamlit Cloud 沒有 data_cache（230MB 不能 push）。
改本機跑此腳本產出 top200_signals.json（小檔可入庫）。
tv_app 讀此 JSON，Cloud 也能即時顯示推薦。
"""
import sys, json
from pathlib import Path
import pandas as pd
import numpy as np
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass


def classify(d):
    e20 = d.get('ema20'); e60 = d.get('ema60')
    if e20 is None or e60 is None: return 'WAIT'
    is_bull = e20 > e60
    rsi = d.get('rsi'); rsi_p = d.get('rsi_prev'); rsi_p2 = d.get('rsi_prev2')
    if not is_bull:
        t4 = (rsi and rsi < 32 and rsi_p and rsi > rsi_p and rsi_p2 and rsi_p > rsi_p2)
        return 'ENTRY' if t4 else 'WAIT'
    adx = d.get('adx')
    if not (adx and adx >= 22): return 'WAIT'
    atr14 = d.get('atr14'); close = d.get('close')
    rel_atr = atr14/close*100 if (atr14 and close) else 0
    gap = (e20-e60)/e60*100 if e60 else None
    if gap and gap < 1.0: return 'EXIT'
    if not (rel_atr > 3.5):
        if adx and adx < 25 and rsi and rsi > 75: return 'EXIT'
    cd = d.get('ema20_cross_days')
    t1 = cd and 0 < cd <= 10
    t3 = rsi and rsi < 50
    return 'ENTRY' if (t1 or t3) else 'HOLD'


def main():
    with open('vwap_applicable.json', encoding='utf-8') as f:
        tier_data = json.load(f)
    top200 = sorted([t for t, info in tier_data.items() if info.get('tier') == 'TOP'])

    name_map = {}
    sj = Path('tw_stock_list.json')
    if sj.exists():
        data = json.load(open(sj, encoding='utf-8'))
        if isinstance(data, dict):
            if 'tickers' in data: data = data['tickers']
            for k, v in data.items():
                if isinstance(v, dict):
                    name_map[k] = v.get('name', '')

    entry, exit_, hold, wait = [], [], [], []
    last_dates = []
    for t in top200:
        p = Path('data_cache') / f'{t}.parquet'
        if not p.exists(): continue
        df = pd.read_parquet(p)
        if len(df) < 30: continue
        last = -1

        e20s, e60s = df['e20'].values, df['e60'].values
        cd = None
        if not (np.isnan(e20s[last]) or np.isnan(e60s[last])):
            cur_bull = e20s[last] > e60s[last]
            for k in range(1, min(60, len(df))):
                if np.isnan(e20s[last-k]) or np.isnan(e60s[last-k]): continue
                if (e20s[last-k] > e60s[last-k]) != cur_bull:
                    cd = k if cur_bull else -k
                    break

        d = {
            'close': float(df['Close'].iloc[last]),
            'ema20': float(e20s[last]) if not np.isnan(e20s[last]) else None,
            'ema60': float(e60s[last]) if not np.isnan(e60s[last]) else None,
            'rsi': float(df['rsi'].iloc[last]) if 'rsi' in df.columns and not np.isnan(df['rsi'].iloc[last]) else None,
            'rsi_prev': float(df['rsi'].iloc[last-1]) if len(df)>=2 and 'rsi' in df.columns and not np.isnan(df['rsi'].iloc[last-1]) else None,
            'rsi_prev2': float(df['rsi'].iloc[last-2]) if len(df)>=3 and 'rsi' in df.columns and not np.isnan(df['rsi'].iloc[last-2]) else None,
            'adx': float(df['adx'].iloc[last]) if 'adx' in df.columns and not np.isnan(df['adx'].iloc[last]) else None,
            'atr14': float(df['atr'].iloc[last]) if 'atr' in df.columns and not np.isnan(df['atr'].iloc[last]) else None,
            'ema20_cross_days': cd,
        }

        # 🆕 PE / PBR / 殖利率（從 per_cache）
        pe_v = pbr_v = div_v = None
        per_path = Path('per_cache') / f'{t}.parquet'
        if per_path.exists():
            try:
                pe_df = pd.read_parquet(per_path)
                if not pe_df.empty:
                    last_per = pe_df.iloc[-1]
                    if 'PER' in pe_df.columns and not pd.isna(last_per.get('PER')):
                        pe_v = round(float(last_per['PER']), 1)
                    if 'PBR' in pe_df.columns and not pd.isna(last_per.get('PBR')):
                        pbr_v = round(float(last_per['PBR']), 2)
                    if 'dividend_yield' in pe_df.columns and not pd.isna(last_per.get('dividend_yield')):
                        div_v = round(float(last_per['dividend_yield']), 2)
            except Exception:
                pass
        action = classify(d)
        delta = tier_data[t].get('delta', 0)
        # 訊號類型描述
        if cd and 0 < cd <= 10:
            sig = f'T1 {cd}d'
        elif d['rsi'] and d['rsi'] < 50:
            sig = f'T3 RSI{d["rsi"]:.0f}'
        elif action == 'EXIT':
            sig = 'RSI>75 / EMA死叉'
        else:
            sig = '—'
        row = {
            'ticker': t,
            'name': name_map.get(t, ''),
            'close': round(d['close'], 2),
            'rsi': round(d['rsi'], 1) if d['rsi'] else None,
            'ema20_cross_days': cd,
            'delta': round(delta, 1),
            'sig': sig,
            'pe': pe_v,
            'pbr': pbr_v,
            'div': div_v,
        }
        if action == 'ENTRY': entry.append(row)
        elif action == 'EXIT': exit_.append(row)
        elif action == 'HOLD': hold.append(row)
        else: wait.append(row)
        last_dates.append(df.index[last].strftime('%Y-%m-%d'))

    # 排序：依 delta（歷史 VWAPEXEC 改善）
    entry.sort(key=lambda x: -x['delta'])
    exit_.sort(key=lambda x: -x['delta'])
    hold.sort(key=lambda x: -x['delta'])

    out = {
        'updated_at': max(last_dates) if last_dates else 'unknown',
        'computed_at': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S'),
        'top200_total': len(top200),
        'entry': entry,
        'exit': exit_,
        'hold': hold,
        'wait_count': len(wait),
    }
    with open('top200_signals.json', 'w', encoding='utf-8') as f:
        json.dump(out, f, indent=2, ensure_ascii=False)

    print(f"📊 TOP 200 即時掃描：")
    print(f"  📅 資料截至：{out['updated_at']}")
    print(f"  🚀 進場：{len(entry)}")
    print(f"  🚪 出倉：{len(exit_)}")
    print(f"  📌 持倉：{len(hold)}")
    print(f"  ⏸  觀望：{len(wait)}")
    print(f"\n✅ 寫入 top200_signals.json")


if __name__ == '__main__':
    main()
