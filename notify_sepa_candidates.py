"""SEPA 候選 LINE 通知（v9.20）
==========================================================
從 screener_results.json 讀「🏆 SEPA Trend Template（7條件+RS≥70）」
+「🏆⭐ Minervini 完整 setup」結果，整理成 LINE 訊息推送。

設計給 weekly_full_scan.yml cron 跑（每天 8 次）使用。

執行
-----
  python notify_sepa_candidates.py            # 推送
  python notify_sepa_candidates.py --dry-run  # 不真推
"""
import sys, os, json, argparse
from pathlib import Path
import urllib.request

try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass


def load_results():
    p = Path('screener_results.json')
    if not p.exists():
        print('❌ screener_results.json 不存在')
        return None
    try:
        return json.load(open(p, encoding='utf-8'))
    except Exception as e:
        print(f'❌ 解析失敗：{e}')
        return None


def push_line(msg: str, dry_run: bool = False):
    token = os.environ.get('LINE_CHANNEL_TOKEN', '')
    user_id = os.environ.get('LINE_USER_ID', '')
    if not token or not user_id:
        print('⚠️ LINE_CHANNEL_TOKEN / LINE_USER_ID 未設定，跳過')
        print(msg)
        return False
    if dry_run:
        print('[DRY RUN] 訊息：')
        print(msg)
        return False

    body = json.dumps({
        'to': user_id,
        'messages': [{'type': 'text', 'text': msg[:4900]}],
    }).encode('utf-8')
    req = urllib.request.Request(
        'https://api.line.me/v2/bot/message/push',
        data=body,
        headers={'Content-Type': 'application/json',
                 'Authorization': f'Bearer {token}'})
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            print(f'✅ LINE 推送成功 (status {r.status})')
            return True
    except Exception as e:
        print(f'❌ LINE 推送失敗：{e}')
        return False


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--dry-run', action='store_true')
    args = p.parse_args()

    d = load_results()
    if not d: return

    bf = d.get('by_filter', {})
    computed_at = d.get('computed_at', '?')

    # 完整 Minervini setup（最高優先）
    minervini_full = bf.get('🏆⭐ Minervini 完整 setup（SEPA+VCP+RS≥70）', [])
    # SEPA + RS≥70
    sepa_rs70 = bf.get('🏆 SEPA Trend Template（7條件+RS≥70）', [])
    # SEPA 7/7（含 RS 未確認）
    sepa_7of7 = bf.get('🏆 SEPA Trend Template 7/7（基礎）', [])
    # VCP 形態
    vcp_pattern = bf.get('📐 VCP 形態（≥2 次收口+接近 pivot）', [])
    # Pivot 接近突破
    pivot_breakout = bf.get('🎯 Pivot 接近突破（VCP+距pivot≤1%）', [])

    if not (minervini_full or sepa_rs70 or vcp_pattern or pivot_breakout):
        print('沒有 SEPA / VCP 候選，不推送')
        return

    lines = ['🏆 SEPA / VCP 候選名單']
    lines.append(f'更新：{computed_at}')
    lines.append('')

    def fmt_row(r):
        flag = '🇹🇼' if r.get('market') == 'tw' else '🇺🇸'
        rs = r.get('rs_rating')
        rs_str = f'RS{rs:.0f}' if rs is not None else '—'
        return (f'  {flag} {r.get("ticker")} {(r.get("name") or "")[:8]} '
                f'${r.get("close"):.2f} {rs_str}')

    def split_market(items):
        tw = [r for r in items if r.get('market') == 'tw']
        us = [r for r in items if r.get('market') == 'us']
        return tw, us

    if minervini_full:
        tw, us = split_market(minervini_full)
        lines.append(f'⭐ Minervini 完整 Setup（SEPA+VCP+RS≥70）')
        for r in (sorted(tw, key=lambda x: -(x.get('rs_rating') or 0))[:5]
                   + sorted(us, key=lambda x: -(x.get('rs_rating') or 0))[:5]):
            lines.append(fmt_row(r))
        lines.append('  → OOS 驗證：🇹🇼 win 47%/+11.2%/82d ⭐')
        lines.append('')

    if sepa_rs70 and not minervini_full:
        tw, us = split_market(sepa_rs70)
        lines.append(f'🏆 SEPA + RS≥70（{len(sepa_rs70)} 檔）')
        for r in (sorted(tw, key=lambda x: -(x.get('rs_rating') or 0))[:5]
                   + sorted(us, key=lambda x: -(x.get('rs_rating') or 0))[:5]):
            lines.append(fmt_row(r))
        lines.append('  → OOS：win 45-51%, mean +6-11%')
        lines.append('')

    if pivot_breakout:
        tw, us = split_market(pivot_breakout)
        lines.append(f'🎯 Pivot 接近突破（VCP+距pivot≤1%）（{len(pivot_breakout)} 檔）')
        for r in (tw[:5] + us[:5]):
            lines.append(fmt_row(r))
        lines.append('')

    if vcp_pattern and not pivot_breakout:
        tw, us = split_market(vcp_pattern)
        lines.append(f'📐 VCP 形態 watchlist（{len(vcp_pattern)} 檔）')
        for r in (tw[:5] + us[:5]):
            lines.append(fmt_row(r))
        lines.append('')

    lines.append('---')
    lines.append('💡 進場建議：fixed_90d hold + -8% stop')
    lines.append('💡 出場時機：跌破 SMA50 連 2 天 + 量增 / 跌破 SMA200')

    msg = '\n'.join(lines)
    push_line(msg, dry_run=args.dry_run)


if __name__ == '__main__':
    main()
