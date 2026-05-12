"""Phase 1 CLI — Sympathy Play Scanner

用法：
    python -m sympathy.scan --date 2026-05-12
    python -m sympathy.scan --date 2026-05-12 --groups AI_Storage_US,AI_Power_US
    python -m sympathy.scan --date 2026-05-12 --json reports/sympathy_2026-05-12.json
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import argparse
import json
import pandas as pd

from sympathy.peer_mapping import get_default_mapping
from sympathy.leader_detector import detect_leaders
from sympathy.laggard_scorer import scan_all_groups


def main():
    p = argparse.ArgumentParser(description='Sympathy Play Scanner (Phase 1 MVP)')
    p.add_argument('--date', type=str, default=None,
                    help='YYYY-MM-DD（預設今日）')
    p.add_argument('--groups', type=str, default=None,
                    help='逗號分隔 group name；預設全部')
    p.add_argument('--json', type=str, default=None,
                    help='輸出 JSON 路徑')
    p.add_argument('--show-leaders', action='store_true',
                    help='只列 leader 不算 laggard')
    args = p.parse_args()

    date = pd.Timestamp(args.date) if args.date else pd.Timestamp.now().normalize()
    mapping = get_default_mapping()
    group_filter = (args.groups.split(',') if args.groups else None)

    # 列出所有 group（供 reference）
    print(f'\n=== Sympathy Play Scanner — {date.strftime("%Y-%m-%d")} ===')
    print(f'\n[Groups loaded] {len(mapping.list_groups())} 個：')
    for g in mapping.list_groups():
        members = mapping.get_members(g)
        print(f'  {g}: {len(members)} 檔 ({mapping.get_description(g)[:30]})')

    # 1. 偵測 leaders
    print(f'\n=== Step 1: Leaders 偵測 ===')
    leaders = detect_leaders(date, mapping, group_filter)
    if not leaders:
        print('(no leader detected)')
    else:
        print(f'{"Ticker":<12s} {"Group":<20s} {"Ret%":>7s} {"VolR":>6s} {"Close":>9s}')
        print('-' * 60)
        for ld in leaders:
            print(f'{ld["ticker"]:<12s} {ld["group"]:<20s} '
                  f'{ld["return_pct"]*100:>+6.2f}% '
                  f'{ld["volume_ratio"]:>6.2f} '
                  f'${ld["close"]:>8.2f}')

    if args.show_leaders or not leaders:
        if args.json:
            os.makedirs(os.path.dirname(args.json) or '.', exist_ok=True)
            with open(args.json, 'w', encoding='utf-8') as f:
                json.dump({'leaders': leaders, 'candidates': []},
                           f, ensure_ascii=False, indent=2, default=str)
            print(f'\n[Saved] {args.json}')
        return

    # 2. 算落後股
    print(f'\n=== Step 2: 補漲候選股 ===')
    candidates = scan_all_groups(date, mapping, group_filter)
    if not candidates:
        print('(no candidates passed filters)')
    else:
        print(f'{"Rank":>4s}  {"Ticker":<10s} {"Group":<18s} {"Leader":<10s} '
              f'{"Score":>6s} {"Corr":>6s} {"SprdP":>6s} {"Lag%":>7s}')
        print('-' * 80)
        for i, c in enumerate(candidates, 1):
            print(f'{i:>4d}  {c["ticker"]:<10s} {(c.get("group") or "?"):<18s} '
                  f'{c["leader"]:<10s} {c["score"]:>6.3f} '
                  f'{c["corr_60d"]:>6.3f} {c["spread_pctile"]:>6.3f} '
                  f'{c["lag_today"]*100:>+6.2f}%')

    if args.json:
        os.makedirs(os.path.dirname(args.json) or '.', exist_ok=True)
        with open(args.json, 'w', encoding='utf-8') as f:
            json.dump({
                'date': date.strftime('%Y-%m-%d'),
                'leaders': leaders,
                'candidates': candidates,
            }, f, ensure_ascii=False, indent=2, default=str)
        print(f'\n[Saved] {args.json}')


if __name__ == '__main__':
    main()
