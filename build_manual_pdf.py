"""產生 MANUAL.pdf — 圖文並茂版使用說明書
================================================
matplotlib 生成研究數據圖表（英文標籤）+ reportlab 中文排版

使用：
  python build_manual_pdf.py
  → 產出 MANUAL.pdf
"""
import sys, io, os
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
                                 Table, TableStyle, PageBreak, Image)
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.cidfonts import UnicodeCIDFont

# 註冊中文字體
pdfmetrics.registerFont(UnicodeCIDFont('STSong-Light'))
CN = 'STSong-Light'

OUT = Path('MANUAL.pdf')
plt.rcParams['axes.unicode_minus'] = False

# ─── 圖表生成 ────────────────────────────────────────
def fig_to_image(fig, w=14*cm, h=8*cm):
    buf = io.BytesIO()
    fig.savefig(buf, format='png', dpi=120, bbox_inches='tight',
                facecolor='white')
    plt.close(fig)
    buf.seek(0)
    return Image(buf, width=w, height=h)


def chart_cross_days():
    """黃金交叉 Day 1-15 × 勝率/RR (TW vs US)"""
    days = list(range(1, 16))
    tw_rr = [0.038, 0.043, 0.042, 0.042, 0.048, 0.051, 0.045, 0.042,
             0.037, 0.038, 0.043, 0.037, 0.042, 0.047, 0.050]
    us_rr = [0.035, 0.026, 0.026, 0.029, 0.029, 0.029, 0.029, 0.031,
             0.031, 0.029, 0.032, 0.032, 0.032, 0.027, 0.028]

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(days, tw_rr, 'o-', color='#3b9eff', linewidth=2,
            markersize=8, label='TW (1058 stocks)')
    ax.plot(days, us_rr, 's-', color='#ff6dc8', linewidth=2,
            markersize=8, label='US (555 high-liquid)')
    # Sweet spot 標記
    ax.axvspan(5, 7, alpha=0.15, color='#3dbb6a')
    ax.text(6, 0.052, 'TW Sweet\nSpot', ha='center', fontsize=10,
            color='#1a8a3a', fontweight='bold')
    ax.axvspan(1, 5, alpha=0.10, color='#ff9944')
    ax.text(3, 0.038, 'US Early\nBird', ha='center', fontsize=9,
            color='#cc6600', fontweight='bold')

    ax.set_xlabel('Days After Golden Cross', fontsize=11)
    ax.set_ylabel('Risk-Reward Ratio (RR)', fontsize=11)
    ax.set_title('Win-Rate by Cross Days (30-day Hold)', fontsize=13)
    ax.legend(loc='lower right')
    ax.grid(True, alpha=0.3)
    ax.set_xticks(days)
    return fig_to_image(fig)


def chart_t3_confidence():
    """T3 信心度 高分 vs 低分 RR"""
    cats = ['T3 Conf 0-1\n(Low)', 'T3 Conf 2-3\n(Mid)', 'T3 Conf 4-5\n(High)']
    rrs = [0.039, 0.048, 0.059]
    colors_b = ['#7a8899', '#c8b87a', '#3dbb6a']

    fig, ax = plt.subplots(figsize=(8, 5))
    bars = ax.bar(cats, rrs, color=colors_b, width=0.5)
    for bar, val in zip(bars, rrs):
        ax.text(bar.get_x() + bar.get_width()/2, val + 0.001,
                f'RR {val:.3f}', ha='center', fontweight='bold')
    ax.set_ylabel('RR (6-year full market)', fontsize=11)
    ax.set_title('T3 Confidence Score (5 EMA conditions) vs RR', fontsize=13)
    ax.set_ylim(0, 0.07)
    ax.grid(True, alpha=0.3, axis='y')
    return fig_to_image(fig)


def chart_drawdown_inverse():
    """跌深反彈 TW vs US 完全相反"""
    cats = ['Drawdown\n>= 15%', 'High Vol\n(ATR>= 5%)',
            'Strict Combo\n(ADX30+RSI50-70+\nNot extended)',
            'Over-extended\n(>3 ATR)', 'ADX < 25\n(Weak)',
            'RSI Overheated\n(>= 70)']
    tw = [-0.000, +0.013, -0.018, +0.015, +0.015, +0.014]
    us = [+0.146, +0.082, +0.033, +0.002, -0.003, +0.000]

    x = np.arange(len(cats))
    w = 0.35
    fig, ax = plt.subplots(figsize=(11, 5.5))
    bars1 = ax.bar(x - w/2, tw, w, label='TW', color='#3b9eff')
    bars2 = ax.bar(x + w/2, us, w, label='US', color='#ff6dc8')
    ax.axhline(0, color='gray', linewidth=0.8)

    # 標差距
    for i, (t, u) in enumerate(zip(tw, us)):
        sign = '←Reverse!' if (t * u < 0 and abs(t-u) > 0.02) else ''
        if sign:
            ax.annotate(sign, xy=(i, max(t, u) + 0.01),
                        ha='center', fontsize=9, color='#cc0000',
                        fontweight='bold')

    ax.set_xticks(x)
    ax.set_xticklabels(cats, fontsize=8)
    ax.set_ylabel('Δ RR vs T1 baseline', fontsize=11)
    ax.set_title('TW vs US: Same Conditions, Opposite Behavior',
                  fontsize=13, fontweight='bold')
    ax.legend()
    ax.grid(True, alpha=0.3, axis='y')
    return fig_to_image(fig)


def chart_strategy_styles():
    """8 個策略風格 TEST RR 對比"""
    styles = ['TW Best\n(P5+VWAP\nEXEC)', 'US Best\n(P10+POS\n+ADX18)',
              'Ultra-Risk\n(5-Layer)', 'Aggressive\n(P0_T1T3)',
              'Risk-Ctrl\n(IND+DXY)', 'Conserv\n(POS+DXY)',
              'RL\nSmart', 'Balanced\n(POS)']
    tw = [0.611, 0.159, 0.241, 0.224, 0.223, 0.198, 0.191, 0.090]
    us = [0.000, 0.496, 0.000, 0.324, 0.000, 0.000, 0.324, 0.341]

    x = np.arange(len(styles))
    w = 0.4
    fig, ax = plt.subplots(figsize=(12, 5.5))
    bars1 = ax.bar(x - w/2, tw, w, label='TW (1058)', color='#3b9eff')
    bars2 = ax.bar(x + w/2, us, w, label='US (555 HL)', color='#ff6dc8')
    # 0 表示無資料（IND/DXY/WRSI/WADX 對 US 無對應）
    for i, u in enumerate(us):
        if u == 0:
            ax.text(i + w/2, 0.01, 'N/A', ha='center', fontsize=7,
                    color='#888888', rotation=90)

    ax.set_xticks(x)
    ax.set_xticklabels(styles, fontsize=8)
    ax.set_ylabel('TEST 22M RR (out-of-sample)', fontsize=11)
    ax.set_title('Strategy Styles × Market: TEST RR Comparison',
                  fontsize=13, fontweight='bold')
    ax.legend()
    ax.grid(True, alpha=0.3, axis='y')
    ax.set_ylim(0, 0.7)
    # 標 ★ 最佳
    ax.text(0, 0.625, '★', ha='center', fontsize=18, color='#ffd700')
    ax.text(1, 0.510, '★', ha='center', fontsize=18, color='#ffd700')
    return fig_to_image(fig)


def chart_per_color_scale():
    """PER 顏色階梯示意"""
    fig, ax = plt.subplots(figsize=(10, 2.5))
    ranges = [('< 0', '#ff5555', 'Loss'),
              ('0-10', '#3dbb6a', 'Cheap'),
              ('10-20', '#3dbb6a', 'Cheap'),
              ('20-30', '#c8b87a', 'Fair'),
              ('30-50', '#e8a020', 'High'),
              ('> 50', '#ff5555', 'Overheat')]
    x = np.arange(len(ranges))
    for i, (label, color, tag) in enumerate(ranges):
        ax.barh(0, 1, left=i, color=color, edgecolor='white', height=1)
        ax.text(i + 0.5, 0, f'{label}\n{tag}', ha='center', va='center',
                fontsize=10, fontweight='bold', color='white')

    ax.set_xlim(0, len(ranges))
    ax.set_ylim(-0.5, 0.5)
    ax.axis('off')
    ax.set_title('PER Color Scale (Valuation)', fontsize=12)
    return fig_to_image(fig, w=14*cm, h=4*cm)


def chart_atr_stop():
    """ATR 停損 4 種倍數示意"""
    cats = ['Inverse ETF\n(×1.5)', 'T4 Bounce\n(×2.0)',
            'Standard\n(×2.5)', 'High ADX≥30\n(×3.0)']
    drops = [-15, -20, -25, -30]
    colors_b = ['#ff5555', '#ff9944', '#7abadd', '#3dbb6a']
    fig, ax = plt.subplots(figsize=(9, 4.5))
    bars = ax.bar(cats, drops, color=colors_b, width=0.5)
    for bar, val in zip(bars, drops):
        ax.text(bar.get_x() + bar.get_width()/2, val - 1,
                f'{val}%', ha='center', fontweight='bold', color='white')
    ax.set_ylabel('Stop-Loss Distance (%)', fontsize=11)
    ax.set_title('ATR Stop-Loss Multiples by Scenario', fontsize=13)
    ax.grid(True, alpha=0.3, axis='y')
    ax.set_ylim(-35, 5)
    ax.axhline(0, color='black', linewidth=0.5)
    return fig_to_image(fig)


def chart_t1_t3_t4_signals():
    """T1/T3/T4 信號 RSI vs ADX 區域"""
    fig, ax = plt.subplots(figsize=(9, 6))
    # T3 拉回（多頭）
    ax.fill_between([0, 50], 22, 50, alpha=0.3, color='#3dbb6a',
                     label='T3 Pullback (Bull + ADX≥22 + RSI<50)')
    # T1 黃金交叉 + ADX≥30 = 飆股
    ax.fill_between([0, 100], 30, 50, alpha=0.5, color='#f0c030',
                     label='Hot (T1 + ADX≥30)')
    # T4 反彈（空頭）
    ax.fill_between([0, 32], 0, 22, alpha=0.4, color='#ff9944',
                     label='T4 Bounce (Bear + RSI<32)')
    # 過熱區
    ax.fill_between([70, 100], 0, 50, alpha=0.2, color='#ff5555',
                     label='Overheat (RSI≥70 — TW: continue! / US: warn)')

    ax.set_xlabel('RSI 14', fontsize=11)
    ax.set_ylabel('ADX 14', fontsize=11)
    ax.set_xlim(0, 100)
    ax.set_ylim(0, 50)
    ax.axvline(50, color='#888', linestyle='--', alpha=0.5)
    ax.axhline(22, color='#888', linestyle='--', alpha=0.5)
    ax.axvline(32, color='#888', linestyle='--', alpha=0.5)
    ax.legend(loc='upper right', fontsize=9)
    ax.set_title('Entry Signal Zones (RSI × ADX)',
                  fontsize=13, fontweight='bold')
    ax.grid(True, alpha=0.3)
    return fig_to_image(fig)


def chart_market_decision_tree():
    """市場環境三狀態決策"""
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.axis('off')
    # 三個狀態方塊
    states = [
        (0.15, 0.5, 'Bull Market\n[OK]\nEMA20 > EMA60\nADX >= Threshold', '#3dbb6a'),
        (0.5, 0.5, 'False Bull\n[!!]\nEMA20 > EMA60\nbut ADX < Threshold', '#e8a020'),
        (0.85, 0.5, 'Bear Market\n[X]\nEMA20 < EMA60', '#ff5555'),
    ]
    for x, y, txt, c in states:
        ax.add_patch(plt.Rectangle((x-0.13, y-0.2), 0.26, 0.4,
                                     facecolor=c, alpha=0.3, edgecolor=c,
                                     linewidth=2))
        ax.text(x, y, txt, ha='center', va='center', fontsize=10,
                fontweight='bold')

    # 動作標記
    actions = [
        (0.15, 0.15, '→ Consider Entry'),
        (0.5, 0.15, '→ No Action'),
        (0.85, 0.15, '→ Watch T4 Only'),
    ]
    for x, y, txt in actions:
        ax.text(x, y, txt, ha='center', fontsize=10, color='#444')

    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.set_title('Market Environment Decision (Section 1)',
                  fontsize=13, fontweight='bold')
    return fig_to_image(fig)


# ─── PDF 內容 ────────────────────────────────────────
def build_manual_pdf():
    doc = SimpleDocTemplate(str(OUT), pagesize=A4,
                             topMargin=2*cm, bottomMargin=2*cm,
                             leftMargin=2*cm, rightMargin=2*cm)
    story = []
    styles = getSampleStyleSheet()
    title_st = ParagraphStyle('T', parent=styles['Title'],
                               fontName=CN, fontSize=22,
                               textColor=colors.HexColor('#3b9eff'),
                               alignment=1, spaceAfter=12)
    h1_st = ParagraphStyle('H1', parent=styles['Heading1'],
                            fontName=CN, fontSize=16,
                            textColor=colors.HexColor('#7abadd'),
                            spaceBefore=14, spaceAfter=8)
    h2_st = ParagraphStyle('H2', parent=styles['Heading2'],
                            fontName=CN, fontSize=12,
                            textColor=colors.HexColor('#3dbb6a'),
                            spaceBefore=8, spaceAfter=4)
    body_st = ParagraphStyle('B', parent=styles['Normal'],
                              fontName=CN, fontSize=10,
                              textColor=colors.HexColor('#222222'),
                              leading=15)
    quote_st = ParagraphStyle('Q', parent=body_st,
                               leftIndent=12, rightIndent=12,
                               textColor=colors.HexColor('#555555'),
                               fontSize=9, leading=13)
    small_st = ParagraphStyle('S', parent=styles['Normal'],
                               fontName=CN, fontSize=8,
                               textColor=colors.HexColor('#666666'),
                               alignment=1)
    note_st = ParagraphStyle('N', parent=body_st,
                              backColor=colors.HexColor('#f0f8ff'),
                              borderColor=colors.HexColor('#7abadd'),
                              borderWidth=1, borderPadding=6,
                              leftIndent=0, rightIndent=0)

    # ── 封面 ──
    story.append(Spacer(1, 4*cm))
    story.append(Paragraph("📖", ParagraphStyle('emoji', fontName=CN,
        fontSize=72, alignment=1, spaceAfter=10)))
    story.append(Paragraph("Stock001 個股分析", title_st))
    story.append(Paragraph("使用說明書", title_st))
    story.append(Spacer(1, 1*cm))
    story.append(Paragraph("圖文並茂版 · v9.10p", small_st))
    story.append(Paragraph(f"生成日期：{datetime.now().strftime('%Y-%m-%d')}",
                            small_st))
    story.append(Spacer(1, 2*cm))
    story.append(Paragraph(
        "看懂 detail 卡片每個區塊與符號的意義<br/>"
        "📌 包含 30,000+ 樣本研究結論<br/>"
        "📌 跨市場 TW vs US 行為差異<br/>"
        "📌 進場/出場/停損 完整決策樹", body_st))
    story.append(PageBreak())

    # ── 目錄 ──
    story.append(Paragraph("📑 目錄", h1_st))
    toc = [
        "1. 快速入門",
        "2. ① 市場環境 (含黃金交叉天數研究)",
        "3. ② 進場判斷 (T1/T3/T4 + 信心度)",
        "4. ③ 出場停損 (ATR 動態計算)",
        "5. ④ 出場獲利 (飆股 vs 穩健股)",
        "6. ⑤ 推薦策略",
        "7. 估值參考 (PER/PBR/殖利率)",
        "8. 新訊號標記 (跌深反彈/高波動)",
        "9. 警告類型",
        "10. 策略風格 (8 個選項雙市場績效)",
        "11. 跨市場行為相反",
        "12. 常見問答",
    ]
    for item in toc:
        story.append(Paragraph(f"• {item}", body_st))
    story.append(PageBreak())

    # ── Ch 1 快速入門 ──
    story.append(Paragraph("1. 🚀 快速入門", h1_st))
    story.append(Paragraph("操作流程", h2_st))
    story.append(Paragraph(
        "<b>Step 1.</b> 側邊欄選「策略風格」<br/>"
        "&nbsp;&nbsp;&nbsp;⭐ TW 最佳 = 搜台股 4 位數 (2330/2454)<br/>"
        "&nbsp;&nbsp;&nbsp;⭐ US 最佳 = 搜美股英文 (AAPL/NVDA)<br/>"
        "<b>Step 2.</b> 在主畫面輸入 ticker → 按「🔍 開始抓取資料」<br/>"
        "<b>Step 3.</b> 看 detail 卡片：① 環境 / ② 進場 / ③④ 停損 / ⑤ 推薦",
        body_st))
    story.append(Paragraph("三個關鍵問句", h2_st))
    story.append(Paragraph(
        "<b>「該不該進場？」</b> → 看 ⑤ 推薦策略<br/>"
        "<b>「進場價位多少合理？」</b> → ② 進場判斷的 T1/T3 條件 + VWAP 提示<br/>"
        "<b>「停損點？」</b> → ③ 出場停損的具體價格", body_st))
    story.append(PageBreak())

    # ── Ch 2 市場環境 ──
    story.append(Paragraph("2. ① 市場環境", h1_st))
    story.append(Paragraph(
        "判斷大方向：多頭 / 空頭 / 假多頭<br/>"
        "由 EMA20 vs EMA60 + ADX 強度決定。", body_st))
    story.append(Spacer(1, 0.3*cm))
    story.append(chart_market_decision_tree())
    story.append(Spacer(1, 0.5*cm))
    story.append(Paragraph("ADX 門檻（跨市場差異）", h2_st))
    adx_data = [
        ['市場', 'ADX 門檻', '研究依據'],
        ['🇹🇼 TW (4 位數)', '≥ 22', 'P5+VWAPEXEC TEST RR 0.611'],
        ['🇺🇸 US (英文)', '≥ 18', 'P10+POS+ADX18 TEST RR 0.496'],
        ['🪙 Crypto (-USD)', '≥ 18', '同 US（v8 研究後 fall-back）'],
    ]
    tbl = Table(adx_data, colWidths=[4*cm, 3*cm, 7*cm])
    tbl.setStyle(TableStyle([
        ('FONTNAME', (0, 0), (-1, -1), CN),
        ('FONTSIZE', (0, 0), (-1, -1), 9),
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#3b9eff')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('GRID', (0, 0), (-1, -1), 0.3, colors.HexColor('#cccccc')),
    ]))
    story.append(tbl)
    story.append(Spacer(1, 0.5*cm))

    story.append(Paragraph("黃金交叉天數研究（核心發現）", h2_st))
    story.append(Paragraph(
        "黃金交叉後不同天數進場的勝率/RR 完全不同！<br/>"
        "TW 與 US 行為相反：", body_st))
    story.append(chart_cross_days())
    story.append(Paragraph(
        "🇹🇼 TW Sweet Spot：<b>Day 5-7</b>（RR 0.051 最高，當天 0.038 最差）<br/>"
        "🇺🇸 US Early Bird：<b>Day 1-5</b>（Day 1 RR 0.035，Day 10 衰減 -17%）<br/>"
        "→ 個股 detail 卡片會自動標記 ⭐ Sweet Spot / ⚡ 早鳥期",
        note_st))
    story.append(PageBreak())

    # ── Ch 3 進場判斷 ──
    story.append(Paragraph("3. ② 進場判斷", h1_st))
    story.append(Paragraph(
        "四種進場觸發類型，依 RSI/ADX 區間判定：", body_st))
    story.append(Spacer(1, 0.3*cm))
    story.append(chart_t1_t3_t4_signals())
    story.append(Spacer(1, 0.3*cm))
    story.append(Paragraph("T3 信心度 5 指標（v9.9t 新增）", h2_st))
    story.append(Paragraph(
        "T3 訊號出現時，系統評估 5 個 EMA 條件命中數：", body_st))
    conf_data = [
        ['#', '指標', '含義'],
        ['1', 'close > EMA20', '收盤站上短期均線'],
        ['2', 'EMA20 5 日上升', '中期均線上行中'],
        ['3', 'EMA5 5 日上升', '短期均線上行中'],
        ['4', 'EMA5 > EMA20', '多頭排列'],
        ['5', 'EMA5 + EMA20 都上升', '雙均線同向'],
    ]
    tbl = Table(conf_data, colWidths=[1*cm, 5*cm, 8*cm])
    tbl.setStyle(TableStyle([
        ('FONTNAME', (0, 0), (-1, -1), CN),
        ('FONTSIZE', (0, 0), (-1, -1), 9),
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#3dbb6a')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('GRID', (0, 0), (-1, -1), 0.3, colors.HexColor('#cccccc')),
    ]))
    story.append(tbl)
    story.append(Spacer(1, 0.3*cm))
    story.append(chart_t3_confidence())
    story.append(Paragraph(
        "高信心 (4-5/5) 比低信心 (0-1/5) <b>RR 高 50%</b>。<br/>"
        "建議：高信心加重部位、低信心縮減。", note_st))
    story.append(PageBreak())

    # ── Ch 4 出場停損 ──
    story.append(Paragraph("4. ③ 出場停損", h1_st))
    story.append(Paragraph(
        "ATR 動態停損 = 收盤 − ATR × 倍數<br/>"
        "依個股波動性自動調整：", body_st))
    story.append(chart_atr_stop())
    story.append(Paragraph("ATR 倍數規則", h2_st))
    atr_data = [
        ['情境', '倍數', '理由'],
        ['一般股', '×2.5', '預設'],
        ['ADX≥30 飆股', '×3.0', '給強趨勢更寬空間'],
        ['反向 ETF', '×1.5', '槓桿產品需嚴格停損'],
        ['T4 反彈', '×2.0', '空頭反彈嚴格控制'],
    ]
    tbl = Table(atr_data, colWidths=[4*cm, 2*cm, 8*cm])
    tbl.setStyle(TableStyle([
        ('FONTNAME', (0, 0), (-1, -1), CN),
        ('FONTSIZE', (0, 0), (-1, -1), 9),
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#7abadd')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('GRID', (0, 0), (-1, -1), 0.3, colors.HexColor('#cccccc')),
    ]))
    story.append(tbl)
    story.append(PageBreak())

    # ── Ch 5 出場獲利 ──
    story.append(Paragraph("5. ④ 出場獲利", h1_st))
    exit_data = [
        ['觸發條件', '出場類型'],
        ['EMA20 死亡交叉 EMA60', '立刻出（持倉首要訊號）'],
        ['ATR×2.5 停損（一般股）', '強制出（風控）'],
        ['ATR×3.0 停損（ADX≥30）', '強趨勢給更寬'],
        ['ATR×1.5 停損（反向 ETF）', '緊縮停損'],
        ['ATR×2.0 停損（T4 反彈）', '嚴格停損'],
        ['RSI > 70', '一般出場'],
        ['RSI > 75 + ADX < 25', '穩健股出場'],
        ['飆股模式（ATR/P>3.5%）', '持到死叉才出（不用 RSI）'],
        ['反向 ETF：RSI > 70', '不限 ADX 直接出'],
        ['T4 反彈：RSI > 55 或 EMA 金叉', 'T4 出場'],
    ]
    tbl = Table(exit_data, colWidths=[7*cm, 8*cm])
    tbl.setStyle(TableStyle([
        ('FONTNAME', (0, 0), (-1, -1), CN),
        ('FONTSIZE', (0, 0), (-1, -1), 9),
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#ff7755')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('ALIGN', (0, 0), (0, -1), 'LEFT'),
        ('ALIGN', (1, 0), (1, -1), 'LEFT'),
        ('GRID', (0, 0), (-1, -1), 0.3, colors.HexColor('#cccccc')),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1),
         [colors.white, colors.HexColor('#fff8f5')]),
    ]))
    story.append(tbl)
    story.append(Spacer(1, 0.5*cm))
    story.append(Paragraph(
        "💡 飆股 (ATR/P>3.5%) 不用 RSI 出場！<br/>"
        "回測：飆股用 RSI>70 出場會砍主升段，回測損失 +400%。"
        "改用「持到 EMA 死叉」策略。", note_st))
    story.append(PageBreak())

    # ── Ch 6 推薦策略 ──
    story.append(Paragraph("6. ⑤ 推薦策略", h1_st))
    story.append(Paragraph(
        "系統根據環境 + 進場 + 風險的綜合建議，分 5 種類別：", body_st))
    rec_data = [
        ['類別', '觸發條件', '建議'],
        ['⑦ 自適應 T3', '多頭 + ADX 達標 + RSI<50',
         '立即進場（拉回）'],
        ['② 趨勢 EMA（飆股）', 'T1 黃金交叉 + ADX≥30',
         '立即進場（不等拉回，持到死叉）'],
        ['⛔ 不進場 即將死叉', 'T1/T3 達成 + EMA20-60 < 1 ATR',
         '觀望避免「進場後立刻出場」'],
        ['⚠️ 不建議進場', '保守風格 + 高波動股 (ATR/P>5%)',
         '縮減部位 / 改選平衡進攻風格'],
        ['等待 T3 拉回', '多頭中段 RSI 50-65',
         '等 RSI<50 再進場'],
    ]
    tbl = Table(rec_data, colWidths=[4*cm, 6*cm, 5*cm])
    tbl.setStyle(TableStyle([
        ('FONTNAME', (0, 0), (-1, -1), CN),
        ('FONTSIZE', (0, 0), (-1, -1), 8),
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#3b9eff')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('GRID', (0, 0), (-1, -1), 0.3, colors.HexColor('#cccccc')),
    ]))
    story.append(tbl)
    story.append(PageBreak())

    # ── Ch 7 估值參考 ──
    story.append(Paragraph("7. 💰 估值參考", h1_st))
    story.append(Paragraph(
        "EPS / PER / PBR / 殖利率 — 提供基本面參考。"
        "PER 用顏色階梯快速判讀：", body_st))
    story.append(chart_per_color_scale())
    story.append(Spacer(1, 0.3*cm))
    val_data = [
        ['指標', 'TW 來源', 'US 來源'],
        ['PER (P/E)', 'per_cache (FinMind)', 'yfinance trailingPE'],
        ['PBR (P/B)', 'per_cache', 'yfinance'],
        ['殖利率', 'per_cache', 'yfinance'],
        ['EPS TTM', '由 PER 反算', 'yfinance trailingEps'],
    ]
    tbl = Table(val_data, colWidths=[4*cm, 5*cm, 6*cm])
    tbl.setStyle(TableStyle([
        ('FONTNAME', (0, 0), (-1, -1), CN),
        ('FONTSIZE', (0, 0), (-1, -1), 9),
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#c8b87a')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('GRID', (0, 0), (-1, -1), 0.3, colors.HexColor('#cccccc')),
    ]))
    story.append(tbl)
    story.append(PageBreak())

    # ── Ch 8 新訊號標記 ──
    story.append(Paragraph("8. ✨ 新訊號標記（v9.10l）", h1_st))
    story.append(Paragraph(
        "研究發現的特殊強訊號 — TW 與 US 邏輯相反！", body_st))
    story.append(chart_drawdown_inverse())
    story.append(Paragraph("🇺🇸 美股專屬", h2_st))
    story.append(Paragraph(
        "<b>📉 跌深反彈訊號 ★★★</b>（最強單一條件）<br/>"
        "條件：多頭 + 從 60d 高點跌 ≥15%<br/>"
        "研究：T1+此條件 RR 0.171（baseline 0.025 → +0.146 / 6.8 倍）<br/><br/>"
        "<b>⚡ 高波動 alpha ★★</b><br/>"
        "條件：多頭 + ATR/P ≥5%<br/>"
        "研究：T1+此條件 RR 0.107（+0.082 / 4.3 倍）", body_st))
    story.append(Paragraph("🇹🇼 台股專屬", h2_st))
    story.append(Paragraph(
        "<b>🚀 強勢延續訊號 ⭐</b><br/>"
        "條件：多頭 + 距 EMA60 >3 ATR（過度延伸）<br/>"
        "研究：RR 0.052（+0.015 / 37%）<br/>"
        "<b>顛覆「過度延伸=危險」直覺，台股是強者恆強</b>", body_st))
    story.append(PageBreak())

    # ── Ch 9 警告類型 ──
    story.append(Paragraph("9. ⚠️ 警告類型", h1_st))
    warn_data = [
        ['類型', '觸發條件', '建議'],
        ['⛔ 即將死叉', 'EMA20-60 < 1 ATR', '觀望，不進場（v9.10j 否決）'],
        ['🔪 接刀風險', '已死叉 + 跌≥15% + %B<0.10', '極高風險，不接'],
        ['⚠️ 弱支撐', '收盤距 EMA60 < 1 ATR', '小跌即停損'],
        ['⚠️ 過度延伸 (TW)', '距 SMA200 > 40%', '一般股危險 / 強勢延續訊號（v9.10l）'],
        ['⚠️ 高波動', 'ATR/P > 5%', 'TW 警告 / US alpha 訊號'],
    ]
    tbl = Table(warn_data, colWidths=[3.5*cm, 5.5*cm, 6*cm])
    tbl.setStyle(TableStyle([
        ('FONTNAME', (0, 0), (-1, -1), CN),
        ('FONTSIZE', (0, 0), (-1, -1), 8),
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#e8a020')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('GRID', (0, 0), (-1, -1), 0.3, colors.HexColor('#cccccc')),
    ]))
    story.append(tbl)
    story.append(PageBreak())

    # ── Ch 10 策略風格 ──
    story.append(Paragraph("10. 🎯 策略風格雙市場績效", h1_st))
    story.append(Paragraph(
        "tv_app 側邊欄 8 個風格的 TEST 22 月 RR 對比：", body_st))
    story.append(chart_strategy_styles())
    story.append(Paragraph(
        "★ 標記 = 該市場最佳<br/>"
        "🇹🇼 TW: ⭐ TW 最佳 (P5+VWAPEXEC) RR 0.611（其他都 < 0.25）<br/>"
        "🇺🇸 US: ⭐ US 最佳 (P10+POS+ADX18) RR 0.496<br/>"
        "搜不同市場的個股請選對應的「⭐」風格", note_st))
    story.append(PageBreak())

    # ── Ch 11 跨市場相反 ──
    story.append(Paragraph("11. 🔄 跨市場行為相反", h1_st))
    story.append(Paragraph(
        "同樣的條件加在 T1 進場，TW 與 US 結果常常相反：", body_st))
    inv_data = [
        ['條件', 'TW Δ RR', 'US Δ RR', '行為'],
        ['過度延伸 (>3 ATR)', '+0.015 ✓', '+0.002', 'TW 順勢 / US 中性'],
        ['ADX<25 弱趨勢', '+0.015 ✓', '-0.003 ✗', '完全相反'],
        ['RSI 過熱 ≥70', '+0.014 ✓', '0.000', 'TW 延續 / US 中性'],
        ['嚴格組合 (ADX30+不延伸)', '-0.018 ✗', '+0.033 ✓', '完全相反'],
        ['接刀 (跌≥15%)', '-0.000', '+0.146 🔥', 'US 大 alpha'],
        ['高波動 (ATR≥5%)', '+0.013', '+0.082 🔥', 'US 強很多'],
        ['RSI 拉回 (30-50)', '-0.011 ✗', '+0.011 ✓', '完全相反'],
    ]
    tbl = Table(inv_data, colWidths=[5*cm, 2.5*cm, 2.5*cm, 5*cm])
    tbl.setStyle(TableStyle([
        ('FONTNAME', (0, 0), (-1, -1), CN),
        ('FONTSIZE', (0, 0), (-1, -1), 8),
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#9d6dff')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('GRID', (0, 0), (-1, -1), 0.3, colors.HexColor('#cccccc')),
    ]))
    story.append(tbl)
    story.append(Spacer(1, 0.5*cm))
    story.append(Paragraph(
        "<b>啟示</b>：<br/>"
        "🇹🇼 TW = 順勢加速型（強者恆強）<br/>"
        "🇺🇸 US = 逆勢回補型（跌深反彈）<br/>"
        "兩市場用相同 T1/T3 觸發但需不同附加條件。",
        note_st))
    story.append(PageBreak())

    # ── Ch 12 常見問答 ──
    story.append(Paragraph("12. ❓ 常見問答", h1_st))
    qas = [
        ("Q1: 為什麼進場條件達成卻顯示「不建議進場」？",
         "三個可能：①即將死叉（EMA20-60 < 1 ATR）②保守風格 vs 高波動 ③過度延伸"),
        ("Q2: 為什麼台股 ADX 22 / 美股 ADX 18？",
         "美股研究 P10+POS+ADX18 寬鬆趨勢效果最佳（RR 0.496），台股維持 ADX22 預設"),
        ("Q3: 飆股模式跟一般股有什麼差別？",
         "飆股：ATR×3.0 + 不用 RSI 出場 + 持到死叉。一般股：ATR×2.5 + RSI>70 出場"),
        ("Q4: 「黃金交叉 6 天前 Sweet Spot」？",
         "TW Day 5-7 RR 0.051 最高 vs Day 1 RR 0.038。等趨勢確認再進場效果好"),
        ("Q5: T3 信心度 4-5/5 vs 0-1/5？",
         "全市場 6 年回測高信心 RR 0.059 vs 低信心 0.039（差 +50%）"),
        ("Q6: 個股 row 顯示 +0% 或 -10%？",
         "色塊 = 過去 60 日價格動量（綠/藍/橙/紅 強→弱）"),
        ("Q7: 「跌深反彈」對台股有效？",
         "無效甚至反向！US RR +0.146 vs TW -0.000，市場邏輯相反"),
        ("Q8: VWAP 進出場建議只對台股？",
         "對。VWAPEXEC 需 5-min bar（玉山 Fugle），美股無此資料"),
        ("Q9: TOP 200 過期 N 天怎辦？",
         "點「🔄 雲端更新」按鈕（~10 秒，yfinance 抓最新）"),
        ("Q10: 個股名稱顯示亂碼或 ticker？",
         "雲端版 name_map 從 us_full_tickers / tw_stock_list 載入"),
    ]
    for q, a in qas:
        story.append(Paragraph(f"<b>{q}</b>", body_st))
        story.append(Paragraph(f"&nbsp;&nbsp;{a}", quote_st))
    story.append(Spacer(1, 1*cm))
    story.append(Paragraph(
        "更詳細請查 INDICATORS.md（指標清單）<br/>"
        "或 PROJECT_STATUS.md（35 條完整研究記錄）",
        small_st))
    story.append(Spacer(1, 1*cm))
    story.append(Paragraph(
        "Stock001 量化研究 / Claude Sonnet 4.5 共同維護<br/>"
        f"v9.10p · {datetime.now().strftime('%Y-%m-%d')}",
        small_st))

    doc.build(story)
    size_kb = OUT.stat().st_size / 1024
    print(f"OK: {OUT} ({size_kb:.1f} KB)")


if __name__ == '__main__':
    build_manual_pdf()
