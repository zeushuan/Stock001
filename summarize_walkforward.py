"""Walk-forward 結果跨市場彙總（讀 walkforward_swing_tw.json + _us.json）
=====================================================================
跑完 `python analyze_swing_dynamic_exit.py --market both --walkforward` 之後執行
回答：哪些 (entry × exit) 組合在 OOS（2024+）仍然有效？
"""
import json, sys
import statistics as stat

try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass


def main():
    try:
        dt = json.load(open('walkforward_swing_tw.json', encoding='utf-8'))['results']
    except FileNotFoundError:
        print("⚠️ walkforward_swing_tw.json 不存在，請先跑 --market tw --walkforward")
        return
    try:
        du = json.load(open('walkforward_swing_us.json', encoding='utf-8'))['results']
    except FileNotFoundError:
        print("⚠️ walkforward_swing_us.json 不存在，僅顯示 TW")
        du = {}

    strat_names = {'A':'A 趨勢延續','B':'B 突破前高','C':'C 拉回EMA20','D':'D 動能加速'}

    # 對每個 (strategy × rule)，平均 train + test stats
    print('='*130)
    print('🌐 跨市場 Walk-forward OOS（TW + US 平均）')
    print('='*130)

    rule_summary = {}
    for market_d, mn in [(dt, 'TW'), (du, 'US')]:
        for strat, rules in market_d.items():
            for rname, r in rules.items():
                key = (strat, rname)
                rule_summary.setdefault(key, {
                    'tr_win': [], 'tr_mean': [], 'te_win': [], 'te_mean': [],
                    'dw': [], 'dm': [], 'status': [],
                })
                rule_summary[key]['tr_win'].append(r['train']['win_pct'])
                rule_summary[key]['tr_mean'].append(r['train']['mean_pct'])
                rule_summary[key]['te_win'].append(r['test']['win_pct'])
                rule_summary[key]['te_mean'].append(r['test']['mean_pct'])
                rule_summary[key]['dw'].append(r['delta_win'])
                rule_summary[key]['dm'].append(r['delta_mean'])
                rule_summary[key]['status'].append(r['status'])

    # 對每個 strategy，按 OOS test mean% 排序（穩健的高報酬最佳）
    for strat, sname in strat_names.items():
        print(f"\n📊 {sname} ({strat}) — TW + US Walk-forward")
        print(f"{'rule':>14}  {'TR win%':>8} {'TR mean%':>9} {'TE win%':>8} {'TE mean%':>9} {'Δwin':>7} {'Δmean':>8}  status")
        print('-'*90)
        rows = []
        for (s, rname), data in rule_summary.items():
            if s != strat: continue
            tr_w = stat.mean(data['tr_win']) if data['tr_win'] else 0
            tr_m = stat.mean(data['tr_mean']) if data['tr_mean'] else 0
            te_w = stat.mean(data['te_win']) if data['te_win'] else 0
            te_m = stat.mean(data['te_mean']) if data['te_mean'] else 0
            dw = te_w - tr_w
            dm = te_m - tr_m
            # 取最差的 status（若 TW=穩定 但 US=嚴重 decay → 取嚴重）
            statuses = data['status']
            if any('嚴重' in s for s in statuses):
                status = '🚨 嚴重 decay'
            elif any('輕微' in s for s in statuses):
                status = '⚠️ 輕微 decay'
            elif all('OOS 更好' in s for s in statuses):
                status = '✅ OOS 更好'
            else:
                status = '✓ 穩定'
            rows.append((rname, tr_w, tr_m, te_w, te_m, dw, dm, status))
        # 按 OOS mean% 排序
        rows.sort(key=lambda x: -x[4])
        for r in rows:
            base = ' [BASE]' if r[0] == 'fixed_30d' else ''
            print(f"{r[0]:>14}  {r[1]:>7.1f}% {r[2]:>+8.2f}% {r[3]:>7.1f}% {r[4]:>+8.2f}% {r[5]:>+6.1f} {r[6]:>+7.2f}  {r[7]}{base}")

    # Top universal recommendations
    print('\n' + '='*130)
    print('🏆 OOS 仍然有效的「穩定」組合（status=穩定 OR OOS 更好，OOS win%≥50）')
    print('='*130)
    survivors = []
    for (strat, rname), data in rule_summary.items():
        statuses = data['status']
        te_w = stat.mean(data['te_win']) if data['te_win'] else 0
        te_m = stat.mean(data['te_mean']) if data['te_mean'] else 0
        if te_w < 50: continue
        if any('嚴重' in s for s in statuses): continue
        survivors.append((strat, rname, te_w, te_m,
                          stat.mean(data['tr_mean']),
                          stat.mean(data['dm'])))
    survivors.sort(key=lambda x: -x[3])
    print(f"{'策略':>8}  {'規則':>14}  {'OOS win%':>9} {'OOS mean%':>10} {'TR mean%':>10} {'Δmean':>8}")
    print('-'*70)
    for s in survivors[:20]:
        print(f"{strat_names.get(s[0], s[0]):>8}  {s[1]:>14}  "
              f"{s[2]:>8.1f}% {s[3]:>+9.2f}% {s[4]:>+9.2f}% {s[5]:>+7.2f}")


if __name__ == '__main__':
    main()
