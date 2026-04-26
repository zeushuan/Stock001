"""分析 P5_T3 vs base 的差異"""
import sys
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import csv
import numpy as np

def load_csv(path):
    d = {}
    for r in csv.DictReader(open(path, encoding='utf-8-sig')):
        try:
            d[r['ticker']] = (float(r['pnl_pct']), int(r['n_trades']), float(r['bh_pct'] or 0))
        except: pass
    return d

import sys
target = sys.argv[1] if len(sys.argv) > 1 else 'results_P5_T3.csv'

base = load_csv('results_base.csv')
p5t3 = load_csv(target)

diffs = []
for tk in base.keys():
    if tk in p5t3:
        b_pct, b_n, bh = base[tk]
        p_pct, p_n, _ = p5t3[tk]
        diffs.append((tk, b_pct, p_pct, p_pct - b_pct, p_n - b_n, bh))

diffs.sort(key=lambda x: x[3], reverse=True)

print('=== TOP 20 改善（P5_T3 比 base 多賺）===')
print(f'{"代號":<8} {"BH%":>10} {"base%":>10} {"P5_T3%":>10} {"差額":>10} {"+交易":>8}')
print('-'*70)
for tk, b, p, d, n_diff, bh in diffs[:20]:
    print(f'  {tk:<7} {bh:>+9.0f}% {b:>+9.0f}% {p:>+9.0f}% {d:>+9.0f}% {n_diff:>+8d}')

print()
print('=== BOTTOM 10 退步 ===')
for tk, b, p, d, n_diff, bh in diffs[-10:]:
    print(f'  {tk:<7} {bh:>+9.0f}% {b:>+9.0f}% {p:>+9.0f}% {d:>+9.0f}% {n_diff:>+8d}')

print()
arr = np.array([d[3] for d in diffs])
print('=== 統計 ===')
print(f'總股票數：{len(diffs)}')
print(f'改善（>+5%）：{np.sum(arr > 5)} 檔  ({np.sum(arr > 5)/len(diffs)*100:.1f}%)')
print(f'不變（-5~+5%）：{np.sum((arr >= -5) & (arr <= 5))} 檔')
print(f'退步（<-5%）：{np.sum(arr < -5)} 檔  ({np.sum(arr < -5)/len(diffs)*100:.1f}%)')
print(f'平均改善：{np.mean(arr):+.2f}%')
if np.sum(arr<-5) > 0:
    print(f'退步股票退步均值：{np.mean(arr[arr < -5]):+.2f}% (n={np.sum(arr<-5)})')
if np.sum(arr>5) > 0:
    print(f'改善股票改善均值：{np.mean(arr[arr > 5]):+.2f}% (n={np.sum(arr>5)})')
