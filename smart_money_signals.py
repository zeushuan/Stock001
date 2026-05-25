"""smart_money_signals — SMS (Smart Money Score) 評分框架
==============================================================

對應規格《Stock001_SmartMoney_Module_Spec.md》§3.4 / §4.3 / §6.1：
  基本面信念分 (0-100) = 機構分數 (0-50) + 內部人分數 (0-50)
  最終 SMS (0-100)     = 信念分 × technical_gate.multiplier (0.3/0.7/1.0)

公開 API:
  - institutional_score(ticker, tracked_funds=...) -> dict (0-50)
  - insider_score(ticker, days=90, min_amount=...)  -> dict (0-50)
  - compute_sms(ticker, d_indicators=None, ...)     -> dict (整合)

行動分級（§6.2）:
  ≥ 75   🟢 強烈關注
  55-74  🟡 觀察候選
  35-54  ⚪ 中性
  < 35   🔴 迴避/警戒

CLI:
  python -m smart_money_signals NVDA            # 完整 SMS
  python -m smart_money_signals NVDA --no-tech  # 只算 fundamental
"""
from __future__ import annotations

import argparse
import logging
import sys
from typing import Optional

import pandas as pd

log = logging.getLogger("stock001.smart_money_signals")


# ── Tracked Funds（規格 §3.1「頂級基金」候選清單）─────────────
# 這些都已驗證可透過 edgartools Company(ticker) 抓到 13F-HR
DEFAULT_TRACKED_FUNDS: list[str] = [
    'BRK-A',  # Berkshire Hathaway — Buffett (210 filings 歷史)
    'BLK',    # BlackRock — 全球最大資產管理
    'BX',     # Blackstone — 另類資產
    'BAC',    # Bank of America — 私人銀行 wealth
    'JPM',    # JPMorgan — asset mgmt
    'STT',    # State Street
    'BEN',    # Franklin Resources
]


# ── 機構分數 (規格 §3.4，0-50) ──────────────────────────────────
def institutional_score(
    ticker: str,
    tracked_funds: Optional[list[str]] = None,
    current_price: Optional[float] = None,
    compare_cache: Optional[dict] = None,
) -> dict:
    """13F 機構分數 0-50（規格 §3.4 四子項 + 紅旗）。

    子項:
      信念度 (Portfolio %)         0-15
      共識度 (同步建倉/加倉家數)   0-15
      淨方向 (整體淨變動)          0-10
      成本距離 (現價 vs 估算成本)  0-10  → 此版本暫設 0（需季度 OHLC）
      紅旗扣分 (Put 部位)          -10/-20

    Args:
        ticker: 目標標的 (e.g. 'NVDA')
        tracked_funds: 要追蹤的基金 ticker 清單；None 用 DEFAULT_TRACKED_FUNDS
        current_price: 用於成本距離計算（若無則該子項 = 0）
        compare_cache: 預先抓好的 dict {fund_ticker: compare_dict}（加速用）

    Returns:
        dict:
          - score: int 0-50（已含紅旗扣分，clip 後）
          - sub_scores: dict 各子項分數
          - tracked: int 追蹤的基金數
          - matches: list[dict] 命中此 ticker 的基金（含 status, value, share_change_pct）
          - red_flag_puts: bool
          - reasons: list[str]
    """
    from data_sources.edgar_13f import fetch_13f_compare, fetch_13f_holdings
    from data_sources.cache import cached_13f_compare, cached_13f_holdings

    funds = tracked_funds or DEFAULT_TRACKED_FUNDS
    matches: list[dict] = []
    reasons: list[str] = []
    red_flag_puts = False
    red_flag_put_funds: list[str] = []
    target_upper = ticker.upper()

    for fund in funds:
        try:
            cmp = (compare_cache or {}).get(fund)
            if cmp is None:
                cmp = cached_13f_compare(fund)
            if 'error' in cmp:
                continue
            delta = cmp.get('delta')
            if not isinstance(delta, pd.DataFrame) or delta.empty:
                continue
            # 只看此標的的列
            if 'Ticker' in delta.columns:
                target_rows = delta[delta['Ticker'].astype(str).str.upper()
                                    == target_upper]
            else:
                target_rows = delta[delta.get('Issuer', '').astype(str).str
                                    .upper().str.contains(target_upper, na=False)]
            if target_rows.empty:
                continue
            for _, row in target_rows.iterrows():
                status = str(row.get('Status', '')).upper()
                value = float(row.get('Value', 0) or 0)
                share_chg_pct = float(row.get('ShareChangePct', 0) or 0)
                # 計算 portfolio %：value / 該 fund 該季總持倉值
                total_val = (delta['Value'].sum()
                             if 'Value' in delta.columns else 0)
                portfolio_pct = (value / total_val * 100
                                 if total_val > 0 else 0)
                matches.append({
                    'fund': fund,
                    'manager': cmp.get('manager_name', fund)[:40],
                    'status': status,
                    'value': value,
                    'portfolio_pct': portfolio_pct,
                    'share_change_pct': share_chg_pct,
                    'is_new': status == 'NEW',
                    'is_increased': status == 'INCREASED',
                })

            # 🆕 紅旗偵測：掃 13F infotable 看有沒有 PUT 在此 ticker 上
            # （規格 §3.3：頂級基金大量買入 Puts → 對沖/做空訊號）
            # 若 caller 提供 compare_cache（測試 / 預抓資料），跳過 Put 偵測
            # 避免多餘網路請求
            if compare_cache is not None:
                continue
            try:
                holdings_recs = cached_13f_holdings(fund, n=1)
                if holdings_recs:
                    htbl = holdings_recs[0].get('holdings')
                    if isinstance(htbl, pd.DataFrame) and 'PutCall' in htbl.columns:
                        # 過濾此 ticker 且 PutCall=='Put'
                        if 'Ticker' in htbl.columns:
                            tk = htbl['Ticker'].astype(str).str.upper()
                            mask = (tk == target_upper) & (
                                htbl['PutCall'].astype(str).str.upper() == 'PUT')
                        else:
                            mask = (htbl.get('Issuer', '').astype(str).str.upper()
                                    .str.contains(target_upper, na=False)
                                    ) & (htbl['PutCall'].astype(str).str.upper() == 'PUT')
                        if mask.any():
                            put_value = float(htbl.loc[mask, 'Value'].sum())
                            red_flag_puts = True
                            red_flag_put_funds.append(f'{fund}(${put_value:,.0f})')
            except Exception as exc:
                log.debug("[institutional_score/puts/%s] %s", fund, exc)
        except Exception as exc:
            log.debug("[institutional_score/%s] %s", fund, exc)
            continue

    # 篩出 NEW + INCREASED（規格 §3.1 兩個維度）
    bullish_matches = [m for m in matches if m['is_new'] or m['is_increased']]
    bearish_matches = [m for m in matches
                       if str(m['status']).upper() in ('CLOSED', 'DECREASED')]

    # ── 子項 1：信念度（v9.51.B 雙軌）─────────────────────
    # 規格 §3.4 原本只看 portfolio %，但 BlackRock/STT 等巨型分散基金
    # 任何個股都 < 1%，導致小型股「信念度」永遠卡 2 分。改雙軌取 max:
    #   abs   : 該股佔該 fund 該季 holdings 比例 (集中型 fund 才會高)
    #   chg   : ShareChangePct (任何 fund 大幅加碼都算強訊號)
    # 例：BRK-A AAPL 9.5% → abs=10，但 BlackRock 對小型股新建倉 +200% → chg=15
    max_pct = max((m['portfolio_pct'] for m in bullish_matches), default=0)
    max_chg = max((m['share_change_pct'] for m in bullish_matches), default=0)

    def _belief_from_abs(p):
        if p >= 10: return 15
        if p >= 5:  return 10
        if p >= 2:  return 5
        if p > 0:   return 2
        return 0

    def _belief_from_chg(p):
        if p >= 100: return 15   # 新建倉 / 翻倍以上 = 強烈重押
        if p >= 50:  return 12
        if p >= 25:  return 8
        if p >= 10:  return 5
        if p > 0:    return 2
        return 0

    s_belief = max(_belief_from_abs(max_pct), _belief_from_chg(max_chg))
    if max_pct > 0 or max_chg > 0:
        reasons.append(
            f'信念度: 最高 portfolio%={max_pct:.2f}% ｜ '
            f'最大 Δshares={max_chg:+.1f}%'
        )

    # ── 子項 2：共識度（bullish 不同基金數）──
    distinct_bullish = len({m['fund'] for m in bullish_matches})
    if distinct_bullish >= 5:
        s_consensus = 15
    elif distinct_bullish >= 3:
        s_consensus = 10
    elif distinct_bullish >= 1:
        s_consensus = 5
    else:
        s_consensus = 0
    if distinct_bullish > 0:
        reasons.append(f'共識度: {distinct_bullish} 家基金 NEW/INCREASED')

    # ── 子項 3：淨方向（bullish vs bearish）──
    n_bull = len(bullish_matches)
    n_bear = len(bearish_matches)
    if n_bull == 0 and n_bear == 0:
        s_direction = 0   # 無任何資料 → 不給分
    elif n_bull > 0 and n_bear == 0:
        s_direction = 10
    elif n_bull > n_bear:
        s_direction = 7
    elif n_bull == n_bear:
        s_direction = 5
    elif n_bear > n_bull:
        s_direction = 0
    else:
        s_direction = 0
    if n_bull + n_bear > 0:
        reasons.append(f'淨方向: {n_bull} 加 / {n_bear} 減')

    # ── 子項 4：成本距離（規格 §3.4） ──
    # 用 yfinance 抓估算建倉季度的 high+low，cost ≈ (H+L)/2
    # 現價 ≤ cost → 10；+0~10% → 7；+10~25% → 3；>+25% → 0
    s_cost = 0
    cost_estimate = None
    if current_price is not None and bullish_matches:
        try:
            cost_estimate = _estimate_avg_purchase_cost(
                ticker, bullish_matches)
            if cost_estimate is not None and cost_estimate > 0:
                dev = (current_price - cost_estimate) / cost_estimate * 100
                if dev <= 0:
                    s_cost = 10
                elif dev <= 10:
                    s_cost = 7
                elif dev <= 25:
                    s_cost = 3
                else:
                    s_cost = 0
                reasons.append(
                    f'成本距離: 估算 ${cost_estimate:.2f} → 現價 '
                    f'${current_price:.2f} ({dev:+.1f}%)'
                )
        except Exception as exc:
            log.debug("[institutional_score/cost] %s", exc)

    # ── 紅旗：(1) Put 部位 (2) 集體 CLOSED ──
    n_closed = sum(1 for m in matches if m['status'] == 'CLOSED')
    red_flag_penalty = 0
    if red_flag_puts:
        red_flag_penalty -= 10
        reasons.append(f'🚨 紅旗: 機構買 Puts: {", ".join(red_flag_put_funds)}')
    if n_closed >= 3:
        red_flag_penalty -= 10
        reasons.append(f'🚨 紅旗: {n_closed} 家集體 CLOSED')
    red_flag_penalty = max(red_flag_penalty, -20)

    raw_score = s_belief + s_consensus + s_direction + s_cost + red_flag_penalty
    score = max(0, min(50, raw_score))

    return {
        'score': int(score),
        'sub_scores': {
            'belief':    s_belief,
            'consensus': s_consensus,
            'direction': s_direction,
            'cost':      s_cost,
            'red_flag':  red_flag_penalty,
        },
        'tracked': len(funds),
        'matches': matches,
        'red_flag_puts': red_flag_puts,
        'red_flag_put_funds': red_flag_put_funds,
        'cost_estimate': cost_estimate,
        'reasons': reasons,
    }


# ── 成本估算 helper ────────────────────────────────────────────
def _estimate_avg_purchase_cost(
    ticker: str,
    bullish_matches: list[dict],
) -> Optional[float]:
    """規格 §3.4 第 4 子項：估算建倉均價 ≈ 該季度 (H + L) / 2。

    用 bullish_matches 第一筆的 fund 推算其當季 13F reporting_period,
    抓 yfinance 該季度 OHLC，計算 (period_high + period_low) / 2。

    若多檔機構在不同季度建倉，取最新的那筆作為估算依據。
    """
    if not bullish_matches:
        return None
    # 用第一個 bullish match 對應的 fund 抓最新一季 13F 取 reporting_period
    fund = bullish_matches[0]['fund']
    try:
        from data_sources.cache import cached_13f_holdings
        recs = cached_13f_holdings(fund, n=1)
        if not recs:
            return None
        period = recs[0].get('reporting_period') or ''
        if not period:
            return None
        # 用 yfinance 抓該季資料
        import yfinance as yf
        from datetime import datetime, timedelta
        # period 通常是季末，例如 '2026-03-31'
        end_dt = datetime.fromisoformat(period[:10])
        start_dt = end_dt - timedelta(days=95)   # 抓整季
        df = yf.Ticker(ticker).history(
            start=start_dt.strftime('%Y-%m-%d'),
            end=(end_dt + timedelta(days=1)).strftime('%Y-%m-%d'),
            auto_adjust=True,
        )
        if df is None or df.empty:
            return None
        h = float(df['High'].max())
        l = float(df['Low'].min())
        return (h + l) / 2
    except Exception as exc:
        log.debug("[_estimate_avg_purchase_cost] %s", exc)
        return None


# ── 內部人分數 (規格 §4.3，0-50) ────────────────────────────────
def insider_score(
    ticker: str,
    days: int = 90,
    min_amount: float = 100_000,
    transactions_df: Optional[pd.DataFrame] = None,
    cluster_result: Optional[dict] = None,
) -> dict:
    """Form 4 內部人分數 0-50（規格 §4.3）。

    前置閘門: 近 days 內無 P (公開市場買入) → score = 0

    子項:
      金額規模    0-15
      集群數      0-20
      職位權重    0-15
      紅旗扣分    -10/-20 (集群賣出)

    Args:
        ticker: 標的
        days: 回看天數 (預設 90)
        min_amount: 集群偵測金額下限
        transactions_df: 預抓 Form 4 交易（None 則自動 fetch）
        cluster_result: 預算 cluster（None 則自動 detect）

    Returns:
        dict:
          - score: int 0-50
          - sub_scores: dict
          - cluster_size / has_cfo / total_value / max_value
          - reasons: list[str]
    """
    from data_sources.edgar_form4 import (
        fetch_form4_transactions, detect_cluster_buying,
    )

    df = transactions_df
    if df is None:
        df = fetch_form4_transactions(ticker, days=days)

    cluster = cluster_result
    if cluster is None:
        cluster = detect_cluster_buying(df, days=30, min_amount=min_amount)

    reasons: list[str] = []

    # ── 前置閘門 ──
    if cluster['cluster_size'] == 0:
        reasons.append(f'前置閘門: 近 {days}d 無 P 買入（規格 §4.3）')
        return {
            'score': 0,
            'sub_scores': {'amount': 0, 'cluster': 0, 'position': 0,
                           'red_flag': 0},
            'cluster_size': 0,
            'has_cfo': False,
            'total_value': 0,
            'max_value': 0,
            'cluster_sells': cluster.get('cluster_sells', 0),
            'reasons': reasons,
        }

    total = cluster['total_value']
    max_v = cluster['max_value']
    n     = cluster['cluster_size']
    cfo   = cluster['has_cfo']

    # ── 子項 1：金額規模（用 max 單筆 + total）──
    use_v = max(max_v, total / max(n, 1))
    if use_v >= 1_000_000:
        s_amount = 15
    elif use_v >= 500_000:
        s_amount = 12
    elif use_v >= 100_000:
        s_amount = 8
    else:
        s_amount = 2
    reasons.append(f'金額: max ${max_v:,.0f}, total ${total:,.0f}')

    # ── 子項 2：集群數 ──
    if n >= 4:
        s_cluster = 20
    elif n >= 3:
        s_cluster = 15
    elif n >= 2:
        s_cluster = 9
    elif n >= 1:
        s_cluster = 4
    else:
        s_cluster = 0
    reasons.append(f'集群: {n} 人')

    # ── 子項 3：職位 ──
    # 從 cluster['insiders'] 判定哪些角色出現
    insiders = cluster.get('insiders', [])
    has_ceo = any('CEO' in (i.get('position') or '').upper()
                  or 'CHIEF EXECUTIVE' in (i.get('position') or '').upper()
                  for i in insiders)
    if cfo:
        s_position = 15
        reasons.append('🎯 「王炸」: 含 CFO')
    elif has_ceo:
        s_position = 9
        reasons.append('含 CEO')
    else:
        s_position = 4

    # ── 紅旗：集群賣出 ──
    cs = cluster.get('cluster_sells', 0)
    red_flag = 0
    if cs >= 5:
        red_flag = -20
        reasons.append(f'🚨 紅旗: cluster sells = {cs} 人')
    elif cs >= 3:
        red_flag = -10
        reasons.append(f'⚠️ 集群賣出: {cs} 人')

    raw = s_amount + s_cluster + s_position + red_flag
    score = max(0, min(50, raw))

    return {
        'score': int(score),
        'sub_scores': {
            'amount': s_amount,
            'cluster': s_cluster,
            'position': s_position,
            'red_flag': red_flag,
        },
        'cluster_size': n,
        'has_cfo': cfo,
        'total_value': total,
        'max_value': max_v,
        'cluster_sells': cs,
        'insiders': insiders,
        'reasons': reasons,
    }


# ── 行動分級（規格 §6.2）─────────────────────────────────────
def action_level(sms: float) -> dict:
    """SMS 0-100 → 行動分級 + 表情符號 + 建議。

    v9.51.B 重新校準（規格原 ≥75 在實務難達成 — 巨型分散基金信念度
    天花板限制 + 內部人 P 罕見）。新門檻基於真實資料分布:
      ≥55  🟢 強烈關注 = 機構共識 + 內部人買 + 技術配合
      ≥40  🟡 觀察候選 = 三項中至少兩項成立
      ≥25  ⚪ 關注候選 = 至少有訊號（如機構或內部人單邊）
      < 25 🔴 迴避     = 無實質訊號 / 紅旗
    """
    if sms >= 55:
        return {'level': 'strong', 'icon': '🟢',
                'label': '強烈關注', 'action': '優先納入加碼候選'}
    if sms >= 40:
        return {'level': 'watch', 'icon': '🟡',
                'label': '觀察候選', 'action': '訊號成立但時機未滿，列入觀察名單'}
    if sms >= 25:
        return {'level': 'mild', 'icon': '⚪',
                'label': '關注候選', 'action': '部分訊號成立，可加入長期觀察清單'}
    return {'level': 'avoid', 'icon': '🔴',
            'label': '迴避/警戒', 'action': '派發階段、紅旗或無實質買入'}


# ── 端到端：compute_sms ─────────────────────────────────────────
def compute_sms(
    ticker: str,
    d_indicators: Optional[dict] = None,
    tracked_funds: Optional[list[str]] = None,
    insider_days: int = 90,
    insider_min_amount: float = 100_000,
) -> dict:
    """完整 SMS 計算（規格 §6.1）。

    Args:
        ticker: 目標標的
        d_indicators: fetch_indicators 輸出的 dict（給 technical_gate 用）
                      若 None，gate multiplier 預設 0.7 (neutral)
        tracked_funds: 機構名單
        insider_days: Form 4 回看天數
        insider_min_amount: 集群偵測金額下限

    Returns:
        {
            'sms': int 0-100
            'action': dict (level/icon/label/action)
            'fundamental': int (機構+內部人, 0-100)
            'institutional': dict (institutional_score 完整輸出)
            'insider':       dict (insider_score 完整輸出)
            'technical':     dict (technical_gate 完整輸出，若 d_indicators 給)
            'multiplier':    float
        }
    """
    cur_price = d_indicators.get('close') if d_indicators else None
    inst = institutional_score(ticker, tracked_funds=tracked_funds,
                                current_price=cur_price)
    ins  = insider_score(ticker, days=insider_days,
                          min_amount=insider_min_amount)
    fundamental = inst['score'] + ins['score']  # 0-100

    # Technical gate
    if d_indicators:
        from t3_scoring import technical_gate
        gate = technical_gate(d_indicators)
        mult = gate['multiplier']
    else:
        gate = None
        mult = 0.7  # 預設 neutral

    sms = int(max(0, min(100, fundamental * mult)))
    return {
        'sms':           sms,
        'action':        action_level(sms),
        'fundamental':   fundamental,
        'institutional': inst,
        'insider':       ins,
        'technical':     gate,
        'multiplier':    mult,
    }


# ── CLI ────────────────────────────────────────────────────────
def _format_report(ticker: str, res: dict) -> str:
    lines = [
        '=' * 70,
        f'  Smart Money Score: {ticker}',
        '=' * 70,
        f'  SMS = {res["sms"]:>3d} / 100   {res["action"]["icon"]} '
        f'{res["action"]["label"]}',
        f'      ↳ {res["action"]["action"]}',
        '',
        f'  計算: ({res["institutional"]["score"]:>2d} + '
        f'{res["insider"]["score"]:>2d}) × {res["multiplier"]:.1f} = '
        f'{res["fundamental"]:>3d} × {res["multiplier"]:.1f} = {res["sms"]}',
    ]
    # 機構
    inst = res['institutional']
    lines += [
        '',
        f'  [機構分數 {inst["score"]:>2d}/50]  (tracked={inst["tracked"]} funds)',
        f'    信念度: {inst["sub_scores"]["belief"]:>2d}/15  '
        f'共識度: {inst["sub_scores"]["consensus"]:>2d}/15  '
        f'淨方向: {inst["sub_scores"]["direction"]:>2d}/10  '
        f'成本: {inst["sub_scores"]["cost"]:>2d}/10  '
        f'紅旗: {inst["sub_scores"]["red_flag"]}',
    ]
    if inst['matches']:
        lines.append('    Matches:')
        for m in inst['matches']:
            lines.append(
                f'      [{m["status"]:9s}] {m["fund"]:6s} '
                f'{m["manager"]:30s}  '
                f'value=${m["value"]:>14,.0f}  '
                f'port%={m["portfolio_pct"]:5.2f}%  '
                f'Δshares={m["share_change_pct"]:+7.1f}%')
    for r in inst['reasons']:
        lines.append(f'      • {r}')

    # 內部人
    ins = res['insider']
    lines += [
        '',
        f'  [內部人分數 {ins["score"]:>2d}/50]',
        f'    金額: {ins["sub_scores"]["amount"]:>2d}/15  '
        f'集群: {ins["sub_scores"]["cluster"]:>2d}/20  '
        f'職位: {ins["sub_scores"]["position"]:>2d}/15  '
        f'紅旗: {ins["sub_scores"]["red_flag"]}',
        f'    cluster_size={ins["cluster_size"]}  has_CFO={ins["has_cfo"]}  '
        f'total=${ins["total_value"]:,.0f}  max=${ins["max_value"]:,.0f}  '
        f'cluster_sells={ins["cluster_sells"]}',
    ]
    for r in ins['reasons']:
        lines.append(f'      • {r}')

    # 技術閘門
    if res['technical']:
        t = res['technical']
        lines += [
            '',
            f'  [技術閘門] phase={t["phase"]}  multiplier={t["multiplier"]}  '
            f'T3={t["t3_score"]}/5',
        ]
        for r in t['reasons']:
            lines.append(f'      • {r}')
    else:
        lines.append('')
        lines.append(f'  [技術閘門] 未啟用 (預設 multiplier=0.7)')

    lines.append('=' * 70)
    return '\n'.join(lines)


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog='smart_money_signals',
        description='Compute Smart Money Score (SMS) for a US stock',
    )
    parser.add_argument('ticker', help='Stock ticker (e.g. NVDA)')
    parser.add_argument('--no-tech', action='store_true',
                        help='Skip technical gate (use multiplier=0.7)')
    parser.add_argument('--insider-days', type=int, default=90)
    parser.add_argument('--min-amount', type=float, default=100_000)
    parser.add_argument('--funds', nargs='+', default=None,
                        help='Override tracked fund tickers')
    args = parser.parse_args(argv)

    # 如有 --no-tech 直接傳 None；否則嘗試用 yfinance + t3_scoring 算 d
    d = None
    if not args.no_tech:
        try:
            import yfinance as yf
            df = yf.Ticker(args.ticker).history(period='1y', auto_adjust=True)
            if len(df) >= 200:
                close = df['Close']
                ema5  = close.ewm(span=5, adjust=False).mean()
                ema20 = close.ewm(span=20, adjust=False).mean()
                sma20 = close.rolling(20).mean()
                sma50 = close.rolling(50).mean()
                sma200 = close.rolling(200).mean()
                delta = close.diff()
                up = delta.clip(lower=0).rolling(14).mean()
                dn = (-delta.clip(upper=0)).rolling(14).mean()
                rsi = float((100 - 100/(1 + up/dn)).iloc[-1])
                d = {
                    'close': float(close.iloc[-1]),
                    'ema5':  float(ema5.iloc[-1]),
                    'ema20': float(ema20.iloc[-1]),
                    'ema5_5d_ago':  float(ema5.iloc[-6]),
                    'ema20_5d_ago': float(ema20.iloc[-6]),
                    'sma20': float(sma20.iloc[-1]),
                    'sma50': float(sma50.iloc[-1]),
                    'sma200': float(sma200.iloc[-1]),
                    'rsi': rsi, 'adx': 20,
                }
        except Exception as exc:
            print(f'  [warn] technical gate disabled: {exc}', file=sys.stderr)

    res = compute_sms(
        args.ticker, d_indicators=d,
        tracked_funds=args.funds,
        insider_days=args.insider_days,
        insider_min_amount=args.min_amount,
    )
    print(_format_report(args.ticker, res))
    return 0


if __name__ == '__main__':
    sys.exit(main())
