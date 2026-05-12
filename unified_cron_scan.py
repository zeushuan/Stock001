"""🆕 v9.25.5：Unified Cron Scan — 一次掃描合併 T1 + Screener + Sympathy

取代 scan_full_t1_cloud.py + screener_full_cloud.py 兩個獨立呼叫：
  - Fetch 只跑一次（省 25-30 分）
  - 從 df_dict 連跑 T1 imminent / Screener filters / Sympathy
  - 寫出 t1_imminent_full.json + screener_results.json + sympathy_latest.json

舊腳本 (scan_full_t1_cloud.py / screener_full_cloud.py) 仍可獨立執行，
不影響本機 dev workflow。
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import time
import json
import pandas as pd
from datetime import datetime
from pathlib import Path

import screener_full_cloud as scr
import scan_full_t1_cloud as t1mod


def main():
    print('=== Unified Cron Scan (v9.25.5) ===')
    print(f'Start: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\n')
    overall_t0 = time.time()

    t1_output = {
        'updated_at': pd.Timestamp.now().strftime('%Y-%m-%d'),
        'computed_at': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S'),
        'description': '全市場 T1 即將上穿掃描（unified）',
        'tw': [], 'us': [],
    }
    scr_output = {
        'updated_at': pd.Timestamp.now().strftime('%Y-%m-%d'),
        'computed_at': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S'),
        'description': '全市場篩選器（unified）',
        'by_filter': {},
        'rs_ratings': {},
    }
    all_combined = {}
    rs_all = {}

    for market in ['tw', 'us']:
        print(f'\n{"="*60}\n📍 Market: {market.upper()}\n{"="*60}')
        # 1. Universe
        uni = scr.get_universe(market)
        if not uni:
            print(f'  no universe for {market}, skip')
            continue
        name_map = scr.load_name_maps(market)

        # 2. Fetch ONCE (refactored fetch_market_data)
        print(f'\n[Fetch] {market.upper()} — 共用此 df_dict 給 T1 + Screener')
        t_fetch = time.time()
        df_dict = scr.fetch_market_data(market, uni)
        print(f'[Fetch] 完成 {time.time()-t_fetch:.1f}s')

        # 3. T1 imminent（從 df_dict）
        print(f'\n[T1] 計算 T1 imminent...')
        t_t1 = time.time()
        try:
            t1_results = t1mod.scan_universe(market, uni, df_dict=df_dict)
            t1_output[market] = t1_results
            print(f'[T1] 完成 {time.time()-t_t1:.1f}s — 找到 {len(t1_results)} 檔候選')
        except Exception as e:
            print(f'[T1] failed: {type(e).__name__}: {e}')

        # 4. Screener filters（從 df_dict）
        print(f'\n[Screener] 跑 filters...')
        t_scr = time.time()
        try:
            results, market_rs = scr.scan_market(market, uni, name_map, df_dict=df_dict)
            for fname, items in results.items():
                all_combined.setdefault(fname, []).extend(items)
            rs_all.update(market_rs)
            print(f'[Screener] 完成 {time.time()-t_scr:.1f}s')
        except Exception as e:
            import traceback; traceback.print_exc()
            print(f'[Screener] failed: {type(e).__name__}: {e}')

    # 5. 寫 T1 + Screener JSON
    scr_output['by_filter'] = all_combined
    scr_output['rs_ratings'] = rs_all

    with open('t1_imminent_full.json', 'w', encoding='utf-8') as f:
        json.dump(t1_output, f, indent=2, ensure_ascii=False)
    print(f'\n✅ 寫入 t1_imminent_full.json')

    # 🆕 v9.12 alike：append history
    try:
        t1mod._append_to_history(t1_output)
    except Exception as e:
        print(f'  (T1 history append fail: {e})')

    with open('screener_results.json', 'w', encoding='utf-8') as f:
        json.dump(scr_output, f, indent=2, ensure_ascii=False)
    print(f'✅ 寫入 screener_results.json')

    # 6. Sympathy（獨立資料源，但只 ~3-5 分鐘）
    print(f'\n[Sympathy] 跑補漲掃描...')
    t_sym = time.time()
    try:
        from sympathy.peer_mapping import get_default_mapping
        from sympathy.leader_detector import detect_leaders
        from sympathy.laggard_scorer import scan_all_groups
        mapping = get_default_mapping()
        as_of = pd.Timestamp.now().normalize()
        leaders = detect_leaders(as_of, mapping)
        candidates = scan_all_groups(as_of, mapping)
        sym_out = {
            'date': as_of.strftime('%Y-%m-%d'),
            'computed_at': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S'),
            'leaders': leaders,
            'candidates': candidates,
        }
        with open('sympathy_latest.json', 'w', encoding='utf-8') as f:
            json.dump(sym_out, f, default=str, indent=2, ensure_ascii=False)
        print(f'[Sympathy] 完成 {time.time()-t_sym:.1f}s — '
              f'{len(leaders)} leaders, {len(candidates)} candidates')
        print(f'✅ 寫入 sympathy_latest.json')
    except Exception as e:
        import traceback; traceback.print_exc()
        print(f'[Sympathy] failed: {type(e).__name__}: {e}')

    total = time.time() - overall_t0
    print(f'\n{"="*60}')
    print(f'🎉 Unified scan 全部完成 — 總耗時 {total/60:.1f} 分鐘')
    print(f'{"="*60}')


if __name__ == '__main__':
    main()
