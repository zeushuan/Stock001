"""Excel 報表輸出（含迷你 RS 線圖 embedding）"""
import os
from typing import List
from datetime import datetime

import pandas as pd
import numpy as np

from scanners.rs_leading_high import RSLeadingHighSignal


def export_signals_to_excel(signals: List[RSLeadingHighSignal],
                              output_path: str,
                              as_of_date=None,
                              embed_charts: bool = False) -> str:
    """輸出 Excel 報表

    包含：
    - signals sheet: 完整訊號表
    - summary sheet: 統計摘要

    Args:
        signals: 訊號清單
        output_path: 輸出 .xlsx 檔案路徑
        embed_charts: 是否嵌入迷你 RS 線圖（需要額外時間）

    Returns:
        最終寫出的檔案路徑
    """
    if not signals:
        # 仍寫一個空白檔案
        df = pd.DataFrame(columns=['ticker', 'note'])
        df.to_excel(output_path, sheet_name='signals', index=False)
        return output_path

    # 主訊號表
    rows = []
    for s in signals:
        rows.append({
            'Rank': s.rank,
            'Ticker': s.ticker,
            'Score': s.quality_score,
            'Theme': s.theme or '',
            'Stock Price': round(s.stock_price, 2),
            'Distance from High %': round(s.stock_distance_from_high_pct * 100, 2),
            'Purple Dots (20d)': s.purple_dot_count_recent,
            'Days Since RS High': s.days_since_rs_high,
            'RS Value': round(s.rs_value, 4),
            'RS Above 21WMA': s.rs_above_wma21,
            'RS 50d Slope Up': s.rs_long_term_trend_up,
            'Volume Ratio': round(s.volume_ratio, 2),
            'Above SMA200': s.above_sma200,
            'Dollar Vol 50d': round(s.dollar_volume_50d, 0) if s.dollar_volume_50d else None,
            'Score: Purple': round(s.score_breakdown.get('purple_dots', 0), 1),
            'Score: RS Slope': round(s.score_breakdown.get('rs_slope', 0), 1),
            'Score: Base Quality': round(s.score_breakdown.get('base_quality', 0), 1),
            'Score: Volume': round(s.score_breakdown.get('volume', 0), 1),
            'Score: Distance': round(s.score_breakdown.get('distance', 0), 1),
        })
    df = pd.DataFrame(rows)

    # Summary
    summary_data = {
        'Metric': [
            'As-of Date', 'Total Signals', 'Mean Score', 'Top Score',
            'Mean Distance from High %', 'Mean Purple Dots',
            'With Theme: AI_storage', 'With Theme: AI_energy',
            'Mean RS Slope', 'Mean Volume Ratio',
        ],
        'Value': [
            (as_of_date.strftime('%Y-%m-%d') if as_of_date else
             signals[0].signal_date.strftime('%Y-%m-%d')),
            len(signals),
            round(np.mean([s.quality_score for s in signals]), 1),
            round(max(s.quality_score for s in signals), 1),
            round(np.mean([s.stock_distance_from_high_pct * 100 for s in signals]), 2),
            round(np.mean([s.purple_dot_count_recent for s in signals]), 1),
            sum(1 for s in signals if s.theme == 'AI_storage'),
            sum(1 for s in signals if s.theme == 'AI_energy'),
            round(np.mean([s.rs_slope_50d for s in signals if s.rs_slope_50d]), 6),
            round(np.mean([s.volume_ratio for s in signals]), 2),
        ]
    }
    df_summary = pd.DataFrame(summary_data)

    os.makedirs(os.path.dirname(output_path) or '.', exist_ok=True)
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        df_summary.to_excel(writer, sheet_name='summary', index=False)
        df.to_excel(writer, sheet_name='signals', index=False)

        # 自動調整欄寬
        for sheet_name in ['summary', 'signals']:
            ws = writer.sheets[sheet_name]
            for col_idx, col in enumerate(
                (df_summary if sheet_name == 'summary' else df).columns, 1):
                max_len = max(
                    len(str(col)),
                    max((len(str(v)) for v in
                         (df_summary if sheet_name == 'summary' else df)[col]),
                        default=10)
                )
                ws.column_dimensions[
                    ws.cell(row=1, column=col_idx).column_letter
                ].width = min(max_len + 2, 30)

    return output_path
