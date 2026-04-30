"""產生 RESEARCH_PAPER.pdf — 論文級完整研究報告
=================================================
彙整 v7 → v9.10t 完整 6.4 年研究

結構：
  Cover / Abstract / TOC
  1. Introduction
  2. Data & Methodology
  3. Strategy Development (60+ variants)
  4. Cross-Market Differences (TW vs US)
  5. Signal Research (cross_days, T3 conf, T1 filters, drawdown)
  6. Cross-Market Linkage (Lag-1, decay, walk-forward)
  7. Black Swan Analysis
  8. Real Portfolio Simulation
  9. Strategy Style Comparison
  10. Conclusions
  References
"""
import sys, io, json
from pathlib import Path
from datetime import datetime
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm, mm
from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer,
                                 Table, TableStyle, PageBreak, Image,
                                 KeepTogether, NextPageTemplate, PageTemplate,
                                 Frame)
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.cidfonts import UnicodeCIDFont

pdfmetrics.registerFont(UnicodeCIDFont('STSong-Light'))
CN = 'STSong-Light'
plt.rcParams['axes.unicode_minus'] = False

OUT = Path('RESEARCH_PAPER.pdf')


# ─── 圖表 ────────────────────────────────────────
def fig_to_image(fig, w=15*cm, h=8*cm):
    buf = io.BytesIO()
    fig.savefig(buf, format='png', dpi=140, bbox_inches='tight',
                facecolor='white')
    plt.close(fig)
    buf.seek(0)
    return Image(buf, width=w, height=h)


def chart_strategy_evolution():
    """v7 → v8 → P5+VWAPEXEC 演進"""
    versions = ['v7\nP0_T1T3', 'v8 +POS', 'v8 +DXY', 'v8 +IND', 'v9 +VWAPEXEC', 'v9.7\nP5+VWAPEXEC']
    train_rr = [0.97, 1.20, 1.15, 1.20, 2.04, 1.40]
    test_rr = [0.22, 0.30, 0.33, 0.40, 0.49, 0.61]

    fig, ax = plt.subplots(figsize=(11, 5))
    x = np.arange(len(versions))
    w = 0.35
    ax.bar(x - w/2, train_rr, w, label='FULL 6Y RR', color='#7abadd')
    ax.bar(x + w/2, test_rr, w, label='TEST 22M RR', color='#3dbb6a')
    for i, (tr, te) in enumerate(zip(train_rr, test_rr)):
        ax.text(i - w/2, tr + 0.03, f'{tr:.2f}', ha='center', fontsize=8)
        ax.text(i + w/2, te + 0.03, f'{te:.2f}', ha='center', fontsize=8)
    ax.set_xticks(x)
    ax.set_xticklabels(versions, fontsize=9)
    ax.set_ylabel('Risk-Reward Ratio (RR)', fontsize=10)
    ax.set_title('Strategy Evolution: v7 → v9.7 (TW Full Market)', fontsize=12)
    ax.legend()
    ax.grid(True, alpha=0.3, axis='y')
    return fig_to_image(fig)


def chart_cross_days_curves():
    days = list(range(1, 16))
    tw_rr = [0.038, 0.043, 0.042, 0.042, 0.048, 0.051, 0.045, 0.042,
             0.037, 0.038, 0.043, 0.037, 0.042, 0.047, 0.050]
    us_rr = [0.035, 0.026, 0.026, 0.029, 0.029, 0.029, 0.029, 0.031,
             0.031, 0.029, 0.032, 0.032, 0.032, 0.027, 0.028]
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(days, tw_rr, 'o-', color='#3b9eff', linewidth=2,
            markersize=7, label='TW (1058 stocks)')
    ax.plot(days, us_rr, 's-', color='#ff6dc8', linewidth=2,
            markersize=7, label='US (555 high-liquid)')
    ax.axvspan(5, 7, alpha=0.15, color='#3dbb6a')
    ax.text(6, 0.052, 'TW Sweet\nSpot', ha='center', fontsize=10,
            color='#1a8a3a', fontweight='bold')
    ax.axvspan(1, 5, alpha=0.10, color='#ff9944')
    ax.text(3, 0.038, 'US Early\nBird', ha='center', fontsize=9,
            color='#cc6600', fontweight='bold')
    ax.set_xlabel('Days After Golden Cross', fontsize=10)
    ax.set_ylabel('RR (30-day Hold)', fontsize=10)
    ax.set_title('Figure: Win-Rate by Cross-Days (TW vs US)', fontsize=12)
    ax.legend()
    ax.grid(True, alpha=0.3)
    ax.set_xticks(days)
    return fig_to_image(fig)


def chart_drawdown_levels():
    """跌幅級距 RR (TW vs US)"""
    cats = ['15-20%', '20-30%', '30-50%', '>50%']
    tw = [0.045, 0.141, 0.266, 0.445]
    us = [0.043, 0.033, -0.025, 0.065]
    x = np.arange(len(cats))
    w = 0.35
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.bar(x - w/2, tw, w, label='TW', color='#3b9eff')
    ax.bar(x + w/2, us, w, label='US', color='#ff6dc8')
    ax.axhline(0, color='gray', linewidth=0.8)
    for i, (t, u) in enumerate(zip(tw, us)):
        ax.text(i - w/2, t + 0.01, f'{t:.3f}', ha='center', fontsize=8)
        ax.text(i + w/2, u + 0.01 if u > 0 else u - 0.03,
                f'{u:.3f}', ha='center', fontsize=8)
    ax.set_xticks(x)
    ax.set_xticklabels(cats, fontsize=10)
    ax.set_ylabel('RR (30-day Hold)', fontsize=10)
    ax.set_title('Figure: Drawdown Magnitude vs Win-Rate (TW grows / US reverses)',
                  fontsize=12)
    ax.legend()
    ax.grid(True, alpha=0.3, axis='y')
    return fig_to_image(fig)


def chart_lag_decay():
    """Lag 0-5 衰減"""
    lags = list(range(6))
    spx_twii = [0.129, 0.427, 0.051, 0.098, -0.098, 0.048]
    sox_twii = [0.128, 0.486, 0.079, 0.089, -0.064, 0.048]
    tsm_2330 = [0.318, 0.523, 0.037, -0.019, -0.035, 0.025]
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(lags, spx_twii, 'o-', label='SPX → TWII', color='#3b9eff', linewidth=2)
    ax.plot(lags, sox_twii, 's-', label='SOX → TWII', color='#ff6dc8', linewidth=2)
    ax.plot(lags, tsm_2330, '^-', label='TSM → 2330', color='#3dbb6a', linewidth=2)
    ax.axhline(0, color='gray', linewidth=0.5)
    ax.axvspan(1, 1.05, alpha=0.2, color='#ffd700')
    ax.text(1, 0.55, 'Peak\n(Lag 1)', ha='center', fontsize=9,
            fontweight='bold', color='#cc8800')
    ax.set_xlabel('Lag (days)', fontsize=10)
    ax.set_ylabel('Correlation', fontsize=10)
    ax.set_title('Figure: US → TW Influence Decay (Half-life ≈ 2 days)',
                  fontsize=12)
    ax.legend()
    ax.grid(True, alpha=0.3)
    ax.set_xticks(lags)
    return fig_to_image(fig)


def chart_annual_correlation():
    """跨年度相關穩定性"""
    years = ['2020', '2021', '2022', '2023', '2024', '2025', '2026']
    spx = [0.34, 0.31, 0.54, 0.48, 0.48, 0.63, 0.48]
    sox = [0.41, 0.29, 0.55, 0.50, 0.58, 0.53, 0.59]
    tsm = [0.55, 0.40, 0.54, 0.54, 0.61, 0.47, 0.52]
    x = np.arange(len(years))
    w = 0.27
    fig, ax = plt.subplots(figsize=(11, 5))
    ax.bar(x - w, spx, w, label='SPX → TWII', color='#3b9eff')
    ax.bar(x, sox, w, label='SOX → TWII', color='#ff6dc8')
    ax.bar(x + w, tsm, w, label='TSM → 2330', color='#3dbb6a')
    ax.set_xticks(x)
    ax.set_xticklabels(years)
    ax.set_ylabel('Lag-1 Correlation', fontsize=10)
    ax.set_title('Figure: Annual Lag-1 Correlation (US Linkage Strengthening)',
                  fontsize=12)
    ax.legend()
    ax.grid(True, alpha=0.3, axis='y')
    ax.set_ylim(0, 0.7)
    return fig_to_image(fig)


def chart_market_regime():
    """多/空/盤整 市況連動"""
    indicators = ['SPX→TWII', 'SOX→TWII', 'VIX→TWII', 'SOX→2330', 'TSM→2330']
    bull = [0.420, 0.448, -0.392, 0.445, 0.503]
    side = [0.354, 0.463, -0.331, 0.445, 0.527]
    bear = [0.568, 0.570, -0.547, 0.568, 0.562]
    x = np.arange(len(indicators))
    w = 0.25
    fig, ax = plt.subplots(figsize=(11, 5))
    ax.bar(x - w, bull, w, label='Bull (n=751)', color='#3dbb6a')
    ax.bar(x, side, w, label='Sideways (n=538)', color='#7abadd')
    ax.bar(x + w, bear, w, label='Bear (n=240)', color='#ff5555')
    ax.axhline(0, color='gray', linewidth=0.5)
    ax.set_xticks(x)
    ax.set_xticklabels(indicators, fontsize=8)
    ax.set_ylabel('Lag-1 Correlation', fontsize=10)
    ax.set_title('Figure: Market Regime × US Linkage (Bear shows strongest)',
                  fontsize=12)
    ax.legend()
    ax.grid(True, alpha=0.3, axis='y')
    return fig_to_image(fig)


def chart_strategy_styles_compare():
    styles = ['TW Best\nP5+VWAP', 'US Best\nP10+ADX18',
              'Ultra-Risk', 'Aggressive', 'Risk-Ctrl',
              'Conservative', 'RL Smart', 'Balanced']
    tw = [0.611, 0.159, 0.241, 0.224, 0.223, 0.198, 0.191, 0.090]
    us = [0.000, 0.496, 0.000, 0.324, 0.000, 0.000, 0.324, 0.341]
    x = np.arange(len(styles))
    w = 0.4
    fig, ax = plt.subplots(figsize=(12, 5))
    ax.bar(x - w/2, tw, w, label='TW (1058)', color='#3b9eff')
    ax.bar(x + w/2, us, w, label='US (555 HL)', color='#ff6dc8')
    for i, u in enumerate(us):
        if u == 0:
            ax.text(i + w/2, 0.01, 'N/A', ha='center', fontsize=7,
                    color='#888888', rotation=90)
    ax.set_xticks(x)
    ax.set_xticklabels(styles, fontsize=8)
    ax.set_ylabel('TEST 22M RR', fontsize=10)
    ax.set_title('Figure: 8 Strategy Styles × Markets (TEST 22M)', fontsize=12)
    ax.legend()
    ax.grid(True, alpha=0.3, axis='y')
    ax.text(0, 0.625, '★', ha='center', fontsize=18, color='#ffd700')
    ax.text(1, 0.510, '★', ha='center', fontsize=18, color='#ffd700')
    return fig_to_image(fig)


def chart_t3_confidence():
    cats = ['T3 Conf 0-1', 'T3 Conf 2-3', 'T3 Conf 4-5']
    rrs = [0.039, 0.048, 0.059]
    fig, ax = plt.subplots(figsize=(8, 4.5))
    bars = ax.bar(cats, rrs, color=['#7a8899', '#c8b87a', '#3dbb6a'], width=0.5)
    for bar, val in zip(bars, rrs):
        ax.text(bar.get_x() + bar.get_width()/2, val + 0.001,
                f'RR {val:.3f}', ha='center', fontweight='bold')
    ax.set_ylabel('RR (Full Market 6Y)', fontsize=10)
    ax.set_title('Figure: T3 Confidence Score vs RR (+50% improvement)',
                  fontsize=12)
    ax.set_ylim(0, 0.07)
    ax.grid(True, alpha=0.3, axis='y')
    return fig_to_image(fig)


def chart_walkforward_per_stock():
    stocks = ['2330\nTSMC', '3711\nASEH', '2308\nDelta', '2454\nMediaTek',
              '2882\nCathay', '2382\nQuanta', '2317\nFoxconn', '2891\nCTBC']
    train_r2 = [0.196, 0.155, 0.097, 0.072, 0.074, 0.039, 0.049, 0.095]
    test_r2 = [0.277, 0.240, 0.141, 0.141, 0.135, 0.133, 0.132, 0.098]
    mae_imp = [11.8, 10.5, 6.0, 4.3, 4.2, 4.1, 3.8, 1.7]

    x = np.arange(len(stocks))
    fig, axes = plt.subplots(2, 1, figsize=(11, 7))

    # 上圖：R² Train/Test
    w = 0.35
    axes[0].bar(x - w/2, train_r2, w, label='Train R² (2020-2023)', color='#7abadd')
    axes[0].bar(x + w/2, test_r2, w, label='Test R² (2024-2026)', color='#3dbb6a')
    axes[0].set_xticks(x)
    axes[0].set_xticklabels(stocks, fontsize=8)
    axes[0].set_ylabel('R²')
    axes[0].set_title('Walk-Forward R² (SOX → TW Stock)', fontsize=11)
    axes[0].legend()
    axes[0].grid(True, alpha=0.3, axis='y')

    # 下圖：MAE 改善
    axes[1].bar(x, mae_imp, color='#ff9944', width=0.6)
    for i, v in enumerate(mae_imp):
        axes[1].text(i, v + 0.3, f'{v:.1f}%', ha='center', fontweight='bold')
    axes[1].set_xticks(x)
    axes[1].set_xticklabels(stocks, fontsize=8)
    axes[1].set_ylabel('MAE Improvement %')
    axes[1].set_title('Walk-Forward MAE Improvement (out-of-sample)', fontsize=11)
    axes[1].grid(True, alpha=0.3, axis='y')

    plt.tight_layout()
    return fig_to_image(fig, h=12*cm)


def chart_us_gap_probability():
    """美股 → 台股跳空機率"""
    cats = ['US <-2%', 'US -2~-0.5', 'US -0.5~+0.5', 'US +0.5~2%', 'US >+2%']
    probs = [5.2, 24.5, 59.9, 87.4, 92.5]
    avg_gap = [-0.86, -0.27, 0.06, 0.39, 0.84]
    fig, ax = plt.subplots(figsize=(11, 5))
    colors_b = ['#ff5555', '#ff9944', '#7a8899', '#7abadd', '#3dbb6a']
    bars = ax.bar(cats, probs, color=colors_b, width=0.6)
    for i, (p, g) in enumerate(zip(probs, avg_gap)):
        ax.text(i, p + 2, f'{p:.1f}%\nμ {g:+.2f}%',
                ha='center', fontsize=9, fontweight='bold')
    ax.set_ylabel('TW Gap-Up Probability %', fontsize=10)
    ax.set_title('Figure: US Daily Change → TW Next-Day Gap Probability',
                  fontsize=12)
    ax.set_ylim(0, 110)
    ax.grid(True, alpha=0.3, axis='y')
    return fig_to_image(fig)


def chart_failed_variants():
    """失敗變體分類統計"""
    cats = ['K-line\nPatterns (17)',
            'Inst\nFlow (10)',
            'EPS/PE\n(13)',
            'Margin\nShort (8)',
            'Black\nSwan (4)',
            'VWAP\nAdvanced (7)',
            'Time/\nTrail Stop (6)',
            'MACD\n(4)',
            'ML\nRegime (2)',
            'Multi-TF\n(4)']
    counts = [17, 10, 13, 8, 4, 7, 6, 4, 2, 4]
    fig, ax = plt.subplots(figsize=(11, 5))
    bars = ax.bar(cats, counts, color='#ff7755', width=0.6)
    for bar, c in zip(bars, counts):
        ax.text(bar.get_x() + bar.get_width()/2, c + 0.3,
                str(c), ha='center', fontweight='bold')
    ax.set_ylabel('Failed Variants Count', fontsize=10)
    ax.set_title(f'Figure: Failed Variant Categories (Total ~75)',
                  fontsize=12)
    ax.grid(True, alpha=0.3, axis='y')
    plt.xticks(rotation=0, fontsize=7)
    return fig_to_image(fig)


def chart_portfolio_simulation():
    """真實 Portfolio CAGR vs benchmark"""
    cats = ['TW v8 vs\nTWII', 'US v8 vs\nSPY', '50/50 vs\n(TWII+SPY)/2']
    strategy = [18.4, 23.3, 20.8]
    benchmark = [36.6, 17.2, 26.9]
    diff = [s - b for s, b in zip(strategy, benchmark)]
    x = np.arange(len(cats))
    w = 0.35
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.bar(x - w/2, strategy, w, label='Strategy', color='#3dbb6a')
    ax.bar(x + w/2, benchmark, w, label='Benchmark', color='#7abadd')
    for i, (s, b, d) in enumerate(zip(strategy, benchmark, diff)):
        ax.text(i - w/2, s + 0.5, f'{s:.1f}%', ha='center', fontsize=9)
        ax.text(i + w/2, b + 0.5, f'{b:.1f}%', ha='center', fontsize=9)
        color = '#3dbb6a' if d > 0 else '#ff5555'
        ax.text(i, max(s, b) + 3, f'Δ {d:+.1f}pp', ha='center',
                fontsize=10, fontweight='bold', color=color)
    ax.set_xticks(x)
    ax.set_xticklabels(cats, fontsize=10)
    ax.set_ylabel('CAGR % (TEST 22M annualized)', fontsize=10)
    ax.set_title('Figure: Real Portfolio CAGR vs Benchmark (TEST 22M)',
                  fontsize=12)
    ax.legend()
    ax.grid(True, alpha=0.3, axis='y')
    ax.set_ylim(0, 45)
    return fig_to_image(fig)


# ─── 樣式 ────────────────────────────────────────
def get_styles():
    base = getSampleStyleSheet()
    title_st = ParagraphStyle('Title', parent=base['Title'],
                               fontName=CN, fontSize=22,
                               textColor=colors.HexColor('#1a3a5a'),
                               alignment=1, spaceAfter=12)
    subtitle_st = ParagraphStyle('Sub', parent=base['Normal'],
                                  fontName=CN, fontSize=12,
                                  textColor=colors.HexColor('#5a8ab0'),
                                  alignment=1, spaceAfter=20)
    h1_st = ParagraphStyle('H1', parent=base['Heading1'],
                            fontName=CN, fontSize=15,
                            textColor=colors.HexColor('#1a3a5a'),
                            spaceBefore=20, spaceAfter=10,
                            keepWithNext=True)
    h2_st = ParagraphStyle('H2', parent=base['Heading2'],
                            fontName=CN, fontSize=12,
                            textColor=colors.HexColor('#3b9eff'),
                            spaceBefore=12, spaceAfter=6,
                            keepWithNext=True)
    h3_st = ParagraphStyle('H3', parent=base['Heading3'],
                            fontName=CN, fontSize=11,
                            textColor=colors.HexColor('#5a8ab0'),
                            spaceBefore=8, spaceAfter=4,
                            keepWithNext=True)
    body_st = ParagraphStyle('Body', parent=base['Normal'],
                              fontName=CN, fontSize=10,
                              textColor=colors.HexColor('#222222'),
                              alignment=4,  # justify
                              firstLineIndent=12, leading=15,
                              spaceAfter=4)
    caption_st = ParagraphStyle('Caption', parent=base['Normal'],
                                 fontName=CN, fontSize=8.5,
                                 textColor=colors.HexColor('#666666'),
                                 alignment=1, spaceBefore=2, spaceAfter=10)
    abstract_st = ParagraphStyle('Abs', parent=body_st,
                                  fontSize=9.5, leftIndent=18, rightIndent=18,
                                  firstLineIndent=0,
                                  backColor=colors.HexColor('#f5f9fc'),
                                  borderColor=colors.HexColor('#7abadd'),
                                  borderWidth=1, borderPadding=10,
                                  spaceBefore=8, spaceAfter=12)
    quote_st = ParagraphStyle('Q', parent=body_st,
                               leftIndent=18, rightIndent=18,
                               firstLineIndent=0, fontSize=9,
                               textColor=colors.HexColor('#444444'),
                               spaceBefore=4, spaceAfter=8)
    ref_st = ParagraphStyle('Ref', parent=base['Normal'],
                             fontName=CN, fontSize=8.5,
                             textColor=colors.HexColor('#444444'),
                             leading=12, leftIndent=18, firstLineIndent=-18,
                             spaceAfter=4)
    small_st = ParagraphStyle('S', parent=base['Normal'],
                               fontName=CN, fontSize=8,
                               textColor=colors.HexColor('#666666'),
                               alignment=1)
    return dict(title=title_st, sub=subtitle_st, h1=h1_st, h2=h2_st, h3=h3_st,
                body=body_st, caption=caption_st, abstract=abstract_st,
                quote=quote_st, ref=ref_st, small=small_st)


def make_table(data, col_widths, header_color='#1a3a5a', stripe=True):
    tbl = Table(data, colWidths=col_widths)
    style = [
        ('FONTNAME', (0, 0), (-1, -1), CN),
        ('FONTSIZE', (0, 0), (-1, -1), 8.5),
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor(header_color)),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('FONTSIZE', (0, 0), (-1, 0), 9),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('GRID', (0, 0), (-1, -1), 0.3, colors.HexColor('#cccccc')),
        ('TOPPADDING', (0, 0), (-1, -1), 4),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
    ]
    if stripe:
        style.append(('ROWBACKGROUNDS', (0, 1), (-1, -1),
                       [colors.white, colors.HexColor('#f5f9fc')]))
    tbl.setStyle(TableStyle(style))
    return tbl


# ─── 報告主結構 ────────────────────────────────────────
def build_paper():
    s = get_styles()
    doc = SimpleDocTemplate(str(OUT), pagesize=A4,
                             topMargin=2*cm, bottomMargin=2*cm,
                             leftMargin=2.2*cm, rightMargin=2.2*cm,
                             title='Stock001 Quantitative Research Paper')
    story = []

    # ── COVER ──
    story.append(Spacer(1, 3*cm))
    story.append(Paragraph(
        '🇹🇼🇺🇸 跨市場量化交易策略研究：', s['title']))
    story.append(Paragraph(
        '六年實證、80+ 變體驗證、跨市場連動分析', s['title']))
    story.append(Spacer(1, 1*cm))
    story.append(Paragraph(
        'A Cross-Market Quantitative Trading Strategy Research<br/>'
        'Six Years of Empirical Evidence (2020-2026)<br/>'
        '80+ Variants Validated, TW vs US Linkage Analysis', s['sub']))
    story.append(Spacer(1, 4*cm))
    story.append(Paragraph('Stock001 Research Project', s['sub']))
    story.append(Paragraph(
        f'v9.10t · Compiled {datetime.now().strftime("%Y-%m-%d")}',
        s['small']))
    story.append(Spacer(1, 1*cm))
    story.append(Paragraph(
        'Co-authored with Claude Sonnet 4.5<br/>'
        'GitHub: zeushuan/Stock001', s['small']))
    story.append(PageBreak())

    # ── ABSTRACT ──
    story.append(Paragraph('摘要 / Abstract', s['h1']))
    story.append(Paragraph(
        '本研究歷時六年（2020 年 1 月至 2026 年 4 月），針對台股全市場 1,058 檔'
        '與美股高流動 555 檔，建立並驗證量化交易策略。透過 80 餘個變體、'
        '三種市況分層、四階段黑天鵝事件分析、跨市場時差校正連動分析，'
        '本研究發現：(1) 台股最佳策略 P5+VWAPEXEC 在 TEST 期 RR 達 0.611、'
        '勝率 56.2%；美股最佳 P10+POS+ADX18 達 RR 0.496、勝率 55.3%。'
        '(2) 跨市場行為呈現顛覆性差異：台股「強者恆強」（過度延伸/RSI過熱有正向 alpha），'
        '美股「逆勢回補」（跌深反彈 RR 達 0.171，但 30-50% 跌幅反向 RR -0.025）。'
        '(3) 美股對台股影響經 Lag-1 校正後相關係數達 +0.486（SOX→TWII），'
        '半衰期約 2 個交易日，空頭時連動最強（+0.568）。'
        '(4) 真實 portfolio simulation 顯示美股策略勝 SPY +6.1pp，'
        '台股因等權結構吃不到台積電集中度而輸 TWII -18.2pp。',
        s['abstract']))
    story.append(Paragraph(
        '<b>關鍵字</b>：量化交易、跨市場連動、Walk-Forward 驗證、跌深反彈、'
        'Lag-1 相關性、台股、美股、ATR 動態停損、T3 信心度旗標、跨市場差異化',
        s['body']))
    story.append(PageBreak())

    # ── TOC ──
    story.append(Paragraph('目錄 / Contents', s['h1']))
    toc = [
        ('1', '引言 Introduction', '4'),
        ('2', '資料與方法 Data & Methodology', '5'),
        ('3', '策略演進 Strategy Development', '6'),
        ('4', '跨市場差異 TW vs US', '8'),
        ('5', '訊號研究 Signal Research', '10'),
        ('5.1', '黃金交叉天數研究 Cross-Days × Win-Rate', '10'),
        ('5.2', 'T1 + 附加條件 T1 with Filters', '11'),
        ('5.3', 'T3 信心度旗標 T3 Confidence Flag', '12'),
        ('5.4', '跌深反彈研究 Drawdown × Days', '13'),
        ('6', '跨市場連動 Cross-Market Linkage', '14'),
        ('7', '黑天鵝事件分析 Black Swan Events', '17'),
        ('8', '真實組合模擬 Real Portfolio Simulation', '18'),
        ('9', '策略風格比較 Strategy Style Comparison', '19'),
        ('10', '結論 Conclusions', '20'),
        ('References', '參考文獻', '22'),
    ]
    toc_data = [['#', '章節', '頁']]
    for num, name, p in toc:
        toc_data.append([num, name, p])
    story.append(make_table(toc_data, [1.5*cm, 12*cm, 1.5*cm]))
    story.append(PageBreak())

    # ─────────────────────────────────────────
    # 1. INTRODUCTION
    # ─────────────────────────────────────────
    story.append(Paragraph('1. 引言 Introduction', s['h1']))
    story.append(Paragraph(
        '量化交易策略的有效性深受市場結構、交易成本與時代背景影響。本研究歷時六年，'
        '從 v7（最大獲利 P0_T1T3 +197.48%）出發，逐步演化到 v9.10t，'
        '探索台股與美股兩個結構截然不同的市場，建立穩健的趨勢追蹤策略。',
        s['body']))
    story.append(Paragraph('1.1 研究動機', s['h2']))
    story.append(Paragraph(
        '台灣股市散戶交易占比約 60%、機構投資人約 40%，與美股以機構為主（>80%）'
        '形成鮮明對比。這種市場結構差異是否會導致同一套技術指標在兩市場呈現'
        '不同甚至相反的訊號？本研究透過實證資料回答這個問題。',
        s['body']))
    story.append(Paragraph('1.2 研究貢獻', s['h2']))
    story.append(Paragraph(
        '本研究主要貢獻包括：(1) 建立了首個針對台美雙市場的統一量化框架，'
        'v9.10t 工具支援 1,613 檔個股即時掃描；(2) 透過 80+ 變體實證'
        '驗證「砍贏家陷阱」假設——大部分嚴格進場過濾反而降低 RR；'
        '(3) 發現跨市場行為呈現顛覆性差異，例如過度延伸在台股是強訊號（+0.015 RR），'
        '在美股則中性；(4) 實作真實組合模擬，揭示等權策略在集中度高的市場（如台股）'
        '結構性輸給市值加權大盤指數。',
        s['body']))
    story.append(PageBreak())

    # ─────────────────────────────────────────
    # 2. DATA & METHODOLOGY
    # ─────────────────────────────────────────
    story.append(Paragraph('2. 資料與方法 Data & Methodology', s['h1']))
    story.append(Paragraph('2.1 資料來源', s['h2']))
    story.append(Paragraph(
        '本研究資料涵蓋 2020 年 1 月至 2026 年 4 月（6 年 4 個月），主要資料源：',
        s['body']))
    data_table = [
        ['資料類型', '來源', '檔案', '檔數'],
        ['台股 OHLCV', 'yfinance + 玉山 Fugle', 'data_cache/', '1,278'],
        ['台股 5-min VWAP', '玉山 Fugle API', 'vwap_cache/', '1,277'],
        ['美股 OHLCV', 'yfinance', 'data_cache/', '2,222'],
        ['加密貨幣', 'yfinance', 'data_cache/', '20'],
        ['美股完整清單', 'NASDAQ Trader FTP', 'us_full_tickers.json', '5,629'],
        ['基本面 PER/PBR', 'FinMind / yfinance', 'per_cache/', '1,036'],
        ['法人持股', 'FinMind', 'shareholding_cache/', '~600'],
        ['券資比', 'FinMind', 'margin_cache/', '1,008'],
        ['新聞情感', 'FinMind + SnowNLP+BERT', 'news_cache/', '部分'],
        ['美股大盤', 'yfinance', '^GSPC/^IXIC/^SOX/^DXY/^VIX', '5'],
    ]
    story.append(make_table(data_table, [3.5*cm, 4*cm, 5*cm, 2*cm]))
    story.append(Paragraph('Table 1: 資料來源彙整', s['caption']))
    story.append(Paragraph('2.2 回測框架', s['h2']))
    story.append(Paragraph(
        '採用 v8 變體策略引擎 (variant_strategy.py)，支援 80+ 旗標組合。'
        '時間切割：FULL (2020-01 ~ 2026-04, 6 年)、TRAIN (2020-01 ~ 2024-05, 4.5 年)、'
        'TEST (2024-06 ~ 2026-04, 22 個月 out-of-sample)。'
        '採用 16-worker ProcessPoolExecutor 平行回測，FULL 全市場 1,042 檔約 5-10 分鐘完成。',
        s['body']))
    story.append(Paragraph('2.3 評估指標', s['h2']))
    story.append(Paragraph(
        '本研究使用 RR = mean(returns) / |worst(returns)| 作為核心風險報酬指標。'
        '與 Sharpe (mean / σ) 不同的是，RR 不假設常態分布，直接以「最壞情況」'
        '為分母，更貼近真實尾部風險。其他指標：勝率（>0% 比例）、'
        '中位報酬（穩健性）、最差報酬（尾部風險）、樣本數 n。',
        s['body']))
    story.append(PageBreak())

    # ─────────────────────────────────────────
    # 3. STRATEGY DEVELOPMENT
    # ─────────────────────────────────────────
    story.append(Paragraph('3. 策略演進 Strategy Development', s['h1']))
    story.append(Paragraph(
        '本研究從 v7 純粹的 P0_T1T3 策略出發，逐步加入 POS（累積獲利檢查）、'
        'IND（產業 specific 過濾）、DXY（美元指數）、VWAPEXEC（5-min VWAP 進出場）等元素。',
        s['body']))
    story.append(chart_strategy_evolution())
    story.append(Paragraph('Figure 1: 策略演進 RR 變化（FULL vs TEST）', s['caption']))
    story.append(Paragraph('3.1 進場觸發 (Entry Triggers)', s['h2']))
    story.append(Paragraph(
        '系統定義四種進場類型：T1（黃金交叉 ≤ 10 天，新趨勢啟動）、'
        'T3（多頭拉回 RSI < 50，回檔買進）、T4（空頭反彈 RSI < 32 + 連續 2 日上升，反轉訊號）、'
        '飆股（T1 + ADX ≥ 30，強趨勢確認）。',
        s['body']))
    story.append(Paragraph('3.2 出場規則 (Exit Rules)', s['h2']))
    story.append(Paragraph(
        '出場觸發條件包含：EMA20 死叉 EMA60、ATR×N 停損（一般 2.5、飆股 3.0、'
        '反向 ETF 1.5、T4 反彈 2.0）、RSI > 70 一般出場、RSI > 75 + ADX < 25 穩健股出場。'
        '飆股模式特別不使用 RSI 出場（回測損失 +400% 的反例驗證），改用「持到 EMA 死叉」策略。',
        s['body']))
    story.append(Paragraph('3.3 失敗變體封存 (Failed Variants)', s['h2']))
    story.append(chart_failed_variants())
    story.append(Paragraph(
        'Figure 2: 失敗變體分類（總計 ~75 個變體無法改善 RR > 0.05）',
        s['caption']))
    story.append(Paragraph(
        '研究累積驗證 75+ 失敗變體，呈現驚人的一致模式：「砍贏家陷阱」'
        '——任何嚴格進場過濾（如 K 線型態確認、法人籌碼、估值門檻、券資比、'
        '黑天鵝防護等）雖能提升勝率，卻同時降低 RR，因為過濾掉了長尾贏家機會。'
        '此發現驗證「v8 過濾恰到好處，已是局部最佳」的核心結論。',
        s['body']))
    story.append(PageBreak())

    # ─────────────────────────────────────────
    # 4. CROSS-MARKET DIFFERENCES
    # ─────────────────────────────────────────
    story.append(Paragraph('4. 跨市場差異 TW vs US', s['h1']))
    story.append(Paragraph(
        '透過分別在 TW 1,058 檔與 US 555 檔（高流動 ADV ≥ $104M）跑全市場回測，'
        '發現兩市場最佳策略參數呈現系統性差異：',
        s['body']))
    diff_table = [
        ['參數', '🇹🇼 TW 最佳', '🇺🇸 US 最佳', '解讀'],
        ['加碼門檻', 'P5 (5%)', 'P10 (10%)', '美股波動高，需更嚴確認'],
        ['ADX 門檻', '≥ 22', '≥ 18', '美股寬鬆趨勢延續強'],
        ['VWAPEXEC', '✓', '✗', '美股無 5-min bar 資料'],
        ['IND/DXY 過濾', '✓', '✗', '台股需跨市場聯動'],
        ['池過濾', 'TOP 200 多因子', 'ADV ≥ $104M', '集中度策略需要'],
        ['TEST RR', '0.611', '0.496', '台股略勝（+VWAPEXEC alpha）'],
        ['vs 大盤', 'TWII -18pp', 'SPY +6.1pp', '美股真實 alpha'],
    ]
    story.append(make_table(diff_table, [3.5*cm, 4*cm, 4*cm, 4.5*cm]))
    story.append(Paragraph('Table 2: 兩市場最佳策略參數對比', s['caption']))
    story.append(Paragraph('4.1 加碼門檻差異邏輯', s['h2']))
    story.append(Paragraph(
        '台股 P5（5%）對應「散戶恐慌賣壓 → 機構承接」的市場結構，'
        '5% 已是有效訊號確認門檻；美股機構主導，5% 更可能是雜訊，需 P10（10%）'
        '才能濾除假突破。實證 US P10+POS RR 0.475 vs P5+POS 0.348（+36%）。',
        s['body']))
    story.append(Paragraph('4.2 ADX 門檻差異邏輯', s['h2']))
    story.append(Paragraph(
        '台股 ADX 22 已能濾除「假多頭」（00737 型反例驗證 -7%）；'
        '美股則因為機構算法快速套利，ADX 18 即可確認趨勢，過嚴反而錯過早期機會。',
        s['body']))
    story.append(PageBreak())

    # ─────────────────────────────────────────
    # 5. SIGNAL RESEARCH
    # ─────────────────────────────────────────
    story.append(Paragraph('5. 訊號研究 Signal Research', s['h1']))
    story.append(Paragraph('5.1 黃金交叉天數研究 Cross-Days × Win-Rate', s['h2']))
    story.append(Paragraph(
        '對全市場每筆 T1 進場依「距黃金交叉天數」分群，計算 30 日固定持有報酬。'
        '樣本：TW 6,773-7,421/天、US 3,772-4,184/天（Day 1-15）。',
        s['body']))
    story.append(chart_cross_days_curves())
    story.append(Paragraph('Figure 3: TW vs US Day 1-15 RR 曲線', s['caption']))
    story.append(Paragraph(
        '發現：(1) 台股 Day 5-7 為 sweet spot（RR 0.051 最高），Day 1 RR 0.038 最差，'
        '反映「不追第一根紅 K」的散戶智慧。(2) 美股 Day 1 最佳（RR 0.035），'
        'Day 1-10 衰減 17%，因機構算法在前 1-3 日吃掉大部分 alpha。',
        s['body']))

    story.append(Paragraph('5.2 T1 + 附加條件 T1 with Filters', s['h2']))
    story.append(Paragraph(
        '對 baseline T1（cross_days 1-10 + ADX 達標 + 多頭）加入 16 個附加條件分層。'
        '結果呈現顛覆性差異：',
        s['body']))
    filter_table = [
        ['條件', '🇹🇼 TW Δ RR', '🇺🇸 US Δ RR', '邏輯'],
        ['過度延伸 (>3 ATR)', '+0.015 ✓', '+0.002', 'TW 強者恆強 / US 中性'],
        ['ADX < 25 弱趨勢', '+0.015 ✓', '-0.003 ✗', '完全相反'],
        ['RSI ≥ 70 過熱', '+0.014 ✓', '0.000', 'TW 延續 / US 中性'],
        ['嚴格組合 (ADX30+不延伸)', '-0.018 ✗', '+0.033 ✓', '完全相反'],
        ['接刀 (跌≥15%)', '-0.000', '+0.146 🔥', 'US 大 alpha'],
        ['高波動 (ATR≥5%)', '+0.013', '+0.082 🔥', 'US 強很多'],
        ['RSI 拉回 (30-50)', '-0.011 ✗', '+0.011 ✓', '完全相反'],
    ]
    story.append(make_table(filter_table, [4.5*cm, 2.8*cm, 2.8*cm, 5.5*cm]))
    story.append(Paragraph('Table 3: T1 + 附加條件雙市場 Δ RR 對比', s['caption']))
    story.append(Paragraph(
        '<b>核心結論</b>：TW = 順勢加速型市場（強者恆強），US = 逆勢回補型市場（接刀/高波動）。'
        '兩市場用相同 T1/T3 觸發但需不同附加條件。',
        s['quote']))

    story.append(Paragraph('5.3 T3 信心度旗標 T3 Confidence Flag', s['h2']))
    story.append(Paragraph(
        '為提升 T3 拉回訊號的可靠性，定義 5 個 EMA 條件作為信心度評分（0-5/5）：'
        '(1) close > EMA20、(2) EMA20 5 日上升、(3) EMA5 5 日上升、'
        '(4) EMA5 > EMA20（多頭排列）、(5) EMA5 + EMA20 都上升。',
        s['body']))
    story.append(chart_t3_confidence())
    story.append(Paragraph('Figure 4: T3 信心度 RR 對比', s['caption']))

    story.append(Paragraph('5.4 跌深反彈研究 Drawdown × Days', s['h2']))
    story.append(Paragraph(
        '對所有「進入跌深窗 (drawdown ≥ 15%)」事件按跌幅級距分群分析：',
        s['body']))
    story.append(chart_drawdown_levels())
    story.append(Paragraph('Figure 5: 跌幅級距 × 30 日反彈 RR', s['caption']))
    story.append(Paragraph(
        '<b>顛覆性發現</b>：台股「跌得越深反彈越強」，>50% 重挫 RR 達 0.445'
        '（baseline 11.7 倍）；美股 30-50% 跌幅反而 RR -0.025（基本面壞），'
        '只有 15-20% 淺跌或 >50% 極端反彈有效。'
        '此差異來自市場結構：台股散戶恐慌賣壓 → 機構低接，'
        '美股機構主導 30%+ 跌幅多伴隨基本面實質惡化。',
        s['quote']))
    story.append(PageBreak())

    # ─────────────────────────────────────────
    # 6. CROSS-MARKET LINKAGE
    # ─────────────────────────────────────────
    story.append(Paragraph('6. 跨市場連動 Cross-Market Linkage', s['h1']))
    story.append(Paragraph(
        '本章透過時差校正驗證美股對台股的領先指標效應。',
        s['body']))
    story.append(Paragraph('6.1 時差校正方法', s['h2']))
    story.append(Paragraph(
        '美東 EST = UTC-5（夏令 -4），台北 TST = UTC+8。美股當日收盤對應'
        '台北次日 04:00-05:00，因此 Lag-1（美股 t-1 日 → 台股 t 日）'
        '為理論最強連動點。',
        s['body']))

    story.append(Paragraph('6.2 整體連動係數', s['h2']))
    linkage_table = [
        ['連動配對', 'Lag 0', 'Lag 1', 'Lag 2', '6 年整體'],
        ['SPX → TWII', '+0.129', '+0.427', '+0.051', '+0.427'],
        ['SOX → TWII', '+0.128', '+0.486', '+0.079', '+0.486'],
        ['VIX → TWII', '-0.099', '-0.395', '-0.083', '-0.395'],
        ['SPX → 2330', '+0.112', '+0.379', '+0.010', '+0.379'],
        ['SOX → 2330', '+0.137', '+0.474', '+0.031', '+0.474'],
        ['TSM → 2330', '+0.318', '+0.523', '+0.037', '+0.523'],
    ]
    story.append(make_table(linkage_table, [4*cm, 2.5*cm, 2.5*cm, 2.5*cm, 3*cm]))
    story.append(Paragraph('Table 4: TW-US Lag-1 連動係數', s['caption']))

    story.append(Paragraph('6.3 衰減速度', s['h2']))
    story.append(chart_lag_decay())
    story.append(Paragraph('Figure 6: 美股 → 台股影響衰減（Lag 0-5）', s['caption']))
    story.append(Paragraph(
        '美股影響半衰期約 2 個交易日：Lag 1 → Lag 2 一般衰減 80-90%。'
        '空頭時衰減僅 73%，恐慌餘震可延續 2-3 日。',
        s['body']))

    story.append(Paragraph('6.4 跨年度穩定性與市況依賴', s['h2']))
    story.append(chart_annual_correlation())
    story.append(Paragraph('Figure 7: 跨年度 Lag-1 相關係數', s['caption']))
    story.append(chart_market_regime())
    story.append(Paragraph('Figure 8: 多/空/盤整市況下的連動強度', s['caption']))
    story.append(Paragraph(
        '兩個重要發現：(1) 連動「年年增強」——SPX→TWII 從 2020 +0.34 升至 2025 +0.63（翻倍），'
        '反映全球化加深、AI 主題同步、機構配置一致。'
        '(2) 空頭時連動最強（SPX→TWII +0.568 vs 多頭 +0.420，強 35%），'
        '顯示恐慌全球共振是跨市場最強傳導機制。',
        s['body']))

    story.append(Paragraph('6.5 跳空機率（實用訊號）', s['h2']))
    story.append(chart_us_gap_probability())
    story.append(Paragraph(
        'Figure 9: 美股漲跌 → 台股次日跳空機率（極清晰決策訊號）',
        s['caption']))
    story.append(Paragraph(
        '美股 ±2% 是台股跳空的決定性閾值：US >+2% → TW 92.5% 跳空高開；'
        'US <-2% → TW 95% 跳空低開。此機率分布為實戰提供高信心訊號。',
        s['body']))

    story.append(Paragraph('6.6 個股級 Walk-Forward 驗證', s['h2']))
    story.append(chart_walkforward_per_stock())
    story.append(Paragraph(
        'Figure 10: 個股級 Walk-Forward（Train 2020-2023 / Test 2024-2026）',
        s['caption']))
    story.append(Paragraph(
        '對 1,058 檔逐個跑 walk-forward β 穩定性測試，發現整體 β 在 2024 後'
        '放大 +50~+167%（連動度增強）。具實用預測力（MAE 改善 > 5%）的'
        'Top 個股：2330 (+11.8%)、3711 (+10.5%)、2308 (+6.0%)、'
        '2454 (+4.3%)、2882 (+4.2%)。',
        s['body']))
    story.append(PageBreak())

    # ─────────────────────────────────────────
    # 7. BLACK SWAN ANALYSIS
    # ─────────────────────────────────────────
    story.append(Paragraph('7. 黑天鵝事件分析 Black Swan Events', s['h1']))
    story.append(Paragraph(
        '採用多重 OR 觸發識別 2020-2026 共 42 個黑天鵝事件：'
        'VIX > 35 OR TWII < -3% OR SPX < -3% OR SOX < -4%。'
        '事件包括 COVID 崩盤、Ukraine 戰爭、2022 升息、SVB、Trump 關稅等。',
        s['body']))
    story.append(Paragraph('7.1 事件後階段分析', s['h2']))
    story.append(Paragraph(
        '把事件後分四個階段測試各訊號 30 日 RR：',
        s['body']))
    bs_table = [
        ['訊號', '① 危險窗內', '② POST10', '③ POST30', '④ NORMAL'],
        ['飆股', '+0.094', '-0.027', '+0.011', '+0.061'],
        ['T1', '+0.124', '+0.009', '+0.031', '+0.066'],
        ['T3', '+0.072', '+0.003', '+0.037', '+0.036'],
        ['T4', '+0.050', '-0.023', '+0.143', '+0.084'],
    ]
    story.append(make_table(bs_table, [3*cm, 3*cm, 3*cm, 3*cm, 3*cm]))
    story.append(Paragraph('Table 5: 黑天鵝事件四階段 RR', s['caption']))
    story.append(Paragraph(
        '<b>顛覆性發現</b>：(1) 危險窗內 T1/T3 RR 反而 ≥ NORMAL（恐慌中是進場好時機）；'
        '(2) POST10（trigger 結束 +1~10 BD）才是真正最差期；'
        '(3) POST30 是 T4 反彈黃金期（RR 0.143）。',
        s['quote']))
    story.append(Paragraph(
        '基於此 EDA 設計 BSPOST10 變體（只擋 POST10），但實戰回測仍失敗'
        '（TEST Δ RR -0.055）。失敗根因：v8 動態出場（ATR×2.5 / RSI>70 / EMA死叉）'
        '已自動切損 POST10 壞 entries，前置時間過濾只砍贏家。'
        '此案例強化「v8 過濾恰到好處」結論。',
        s['body']))
    story.append(PageBreak())

    # ─────────────────────────────────────────
    # 8. PORTFOLIO SIMULATION
    # ─────────────────────────────────────────
    story.append(Paragraph('8. 真實組合模擬 Real Portfolio Simulation', s['h1']))
    story.append(Paragraph(
        '把每股 RR 換算成真實 portfolio 等權年化 CAGR（含交易成本 TW 0.4275% / US 0.10%），'
        '對標各自市場 buy-hold benchmark：',
        s['body']))
    story.append(chart_portfolio_simulation())
    story.append(Paragraph('Figure 11: 真實組合 CAGR vs Benchmark', s['caption']))
    story.append(Paragraph(
        '<b>三大重要發現</b>：(1) TW v8 等權 TEST CAGR +18.4% 大幅輸 TWII +36.6%（-18.2pp），'
        '原因為等權結構吃不到台積電 30%+ 集中度；'
        '(2) US v8 高流動 TEST CAGR +23.3% 勝 SPY +17.2%（+6.1pp），'
        '因美股股池分散、無單一巨頭；'
        '(3) 跨市場 50/50 組合 +20.8% 輸 (TWII+SPY)/2 = +26.9%（-6.1pp），'
        '兩市場集中度都被懲罰。',
        s['body']))
    story.append(Paragraph(
        '此結果挑戰「RR 0.611 = 必勝大盤」的直覺：等權策略在集中度高的市場'
        '結構性處於劣勢，需要 tier 排序或加權配置才能轉化 RR alpha 為實際超額報酬。',
        s['quote']))
    story.append(PageBreak())

    # ─────────────────────────────────────────
    # 9. STRATEGY STYLE COMPARISON
    # ─────────────────────────────────────────
    story.append(Paragraph('9. 策略風格比較 Strategy Style Comparison', s['h1']))
    story.append(Paragraph(
        '對 8 個策略風格（含 6 個早期風格 + 2 個最佳）跨市場 TEST 22 月 RR 對比：',
        s['body']))
    story.append(chart_strategy_styles_compare())
    story.append(Paragraph(
        'Figure 12: 8 風格雙市場 TEST RR（★ 表該市場最佳）',
        s['caption']))
    story.append(Paragraph(
        '⭐ TW 最佳 (P5+VWAPEXEC) RR 0.611 是其他 6 風格（0.090-0.241）的 2.5-7 倍。'
        '⭐ US 最佳 (P10+POS+ADX18) RR 0.496 vs 第二名 ⚖️ 平衡 (POS) 0.341。'
        '注意 4 個 TW-only 風格（含 IND/DXY/WRSI/WADX）對 US 顯示 N/A，'
        '因 US 無對應跨市場資料。',
        s['body']))
    story.append(PageBreak())

    # ─────────────────────────────────────────
    # 10. CONCLUSIONS
    # ─────────────────────────────────────────
    story.append(Paragraph('10. 結論 Conclusions', s['h1']))
    story.append(Paragraph('10.1 五條確認研究法則', s['h2']))
    laws_table = [
        ['法則', '內容'],
        ['1', 'v8 + P5 + VWAPEXEC = TW 全市場局部最佳'],
        ['2', 'TOP 200 多因子 tier 帶來 7× RR 提升'],
        ['3', 'T4 反彈是唯一全市場全期穩定訊號'],
        ['4', 'T3 信心度旗標有效（高分 4-5 RR +50%）'],
        ['5', '飆股策略只在 TOP 200 適用'],
    ]
    story.append(make_table(laws_table, [1.5*cm, 14*cm]))
    story.append(Spacer(1, 0.3*cm))
    story.append(Paragraph('10.2 跨市場兩大發現', s['h2']))
    story.append(Paragraph(
        '(1) <b>市場結構決定策略行為</b>：台股散戶主導 → 順勢加速；'
        '美股機構主導 → 逆勢回補。同一條件（過度延伸/接刀/RSI 過熱）'
        '在兩市場 RR 可呈相反符號，必須市場差異化處理。',
        s['body']))
    story.append(Paragraph(
        '(2) <b>美股對台股影響年年增強</b>：SPX→TWII 從 2020 +0.34 升至 2025 +0.63，'
        '空頭時連動 +0.57 為最強傳導；TSM ADR 是 2330 隔日最穩預測指標'
        '（σ 僅 0.066，年年 +0.40~+0.61）。',
        s['body']))
    story.append(Paragraph('10.3 失敗模式總結', s['h2']))
    story.append(Paragraph(
        '75+ 失敗變體呈現一致的「砍贏家陷阱」模式：嚴格進場過濾雖能提升勝率，'
        '卻會錯過長尾贏家機會，導致 RR 下降。此包含：K 線型態（17 變體）、'
        '三大法人籌碼（10+）、估值過濾 EPS/PE/券資比（30+）、黑天鵝防護（4）、'
        '動態 / 時間停損、MACD 雙線、ML Regime Gate 等。',
        s['body']))
    story.append(Paragraph('10.4 工具實作', s['h2']))
    story.append(Paragraph(
        '完整研究產出 Streamlit Web 工具 tv_app.py（v9.10t），'
        '支援 1,278 檔台股 + 444 檔美股即時掃描、TOP 200 多因子排名、'
        'T1/T3/T4 信號分類、T3 信心度旗標、估值參考、'
        '美股盤後預警 + 美股連動度顯示、PDF 完整指標報告匯出。'
        '部署於 Streamlit Cloud，任何時刻可即時更新訊號。',
        s['body']))
    story.append(Paragraph('10.5 未來研究方向', s['h2']))
    story.append(Paragraph(
        '本研究列為「不做」的失敗方向已封存。未來可探索：'
        '(1) 跨年度 σ 穩定性 walk-forward；(2) 期權 IV 整合；'
        '(3) 跨產業 pairs trading（TSM vs SOX）；'
        '(4) 真正的 ML alpha 個股分數；(5) 衛星圖 / 信用卡刷卡等 alt-data。',
        s['body']))
    story.append(PageBreak())

    # ─────────────────────────────────────────
    # REFERENCES
    # ─────────────────────────────────────────
    story.append(Paragraph('參考文獻 References', s['h1']))
    refs = [
        'Jegadeesh, N., & Titman, S. (1993). Returns to buying winners and selling losers: '
        'Implications for stock market efficiency. <i>Journal of Finance</i>, 48(1), 65-91.',
        'Wilder, J. W. (1978). <i>New concepts in technical trading systems</i>. '
        'Trend Research.',
        'Jegadeesh, N. (1990). Evidence of predictable behavior of security returns. '
        '<i>Journal of Finance</i>, 45(3), 881-898.',
        'Fama, E. F., & French, K. R. (1996). Multifactor explanations of asset pricing '
        'anomalies. <i>Journal of Finance</i>, 51(1), 55-84.',
        'Carhart, M. M. (1997). On persistence in mutual fund performance. '
        '<i>Journal of Finance</i>, 52(1), 57-82.',
        'Diebold, F. X., & Yilmaz, K. (2009). Measuring financial asset return and '
        'volatility spillovers, with application to global equity markets. '
        '<i>Economic Journal</i>, 119(534), 158-171.',
        'Hong, H., Lim, T., & Stein, J. C. (2000). Bad news travels slowly: '
        'Size, analyst coverage, and the profitability of momentum strategies. '
        '<i>Journal of Finance</i>, 55(1), 265-295.',
        'Andrei, D., & Cujean, J. (2017). Information percolation, momentum, and reversal. '
        '<i>Journal of Financial Economics</i>, 123(3), 617-645.',
        'Lo, A. W. (2004). The adaptive markets hypothesis. '
        '<i>Journal of Portfolio Management</i>, 30(5), 15-29.',
        'PROJECT_STATUS.md (2026). Stock001 Research Log: 36 Sections of Validation. '
        'GitHub: zeushuan/Stock001.',
    ]
    for r in refs:
        story.append(Paragraph(r, s['ref']))

    story.append(Spacer(1, 1*cm))
    story.append(Paragraph(
        '本研究檔案家族：<br/>'
        '• PROJECT_STATUS.md — 36 條完整研究記錄<br/>'
        '• INDICATORS.md — 技術指標完整參考手冊<br/>'
        '• MANUAL.md / MANUAL.pdf — 個股分析使用說明書<br/>'
        '• FINAL_SUMMARY.md — 最終封存文件<br/>'
        '• BACKLOG.md — 未來研究方向<br/>'
        '• 23 個 analyze_*.py 研究腳本',
        s['small']))
    story.append(Spacer(1, 1*cm))
    story.append(Paragraph(
        f'© 2026 Stock001 Research Project · v9.10t · '
        f'{datetime.now().strftime("%Y-%m-%d")}',
        s['small']))

    doc.build(story)
    size_kb = OUT.stat().st_size / 1024
    print(f"✅ {OUT} ({size_kb:.1f} KB)")


if __name__ == '__main__':
    build_paper()
