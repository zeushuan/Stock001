"""T3 信心評分系統整合介面

把 RSLeadingHighSignal 清單轉成 T3 可吸收的 JSON schema。

T3 期望格式（推測 — 與 Eddy 確認後可調整）：
{
    "metadata": {...},
    "features": [
        {
            "ticker": "NVDA",
            "asof_date": "2026-05-12",
            "technical": {
                "rs_leading_high_score": 85.5,
                "rs_value": ..., "purple_dots": ...,
                ...
            },
            "themes": [...],
        },
        ...
    ]
}
"""
import os
import json
from datetime import datetime
from typing import List, Dict, Any

from scanners.rs_leading_high import RSLeadingHighSignal, signal_to_dict


T3_SCHEMA_VERSION = "1.0"


def export_signals_to_t3(signals: List[RSLeadingHighSignal],
                          output_path: str,
                          extra_metadata: Dict[str, Any] = None) -> str:
    """匯出訊號為 T3 標準 JSON 格式

    Args:
        signals: RSLeadingHighSignal 清單（已排序）
        output_path: 輸出檔案路徑
        extra_metadata: 額外的 metadata（會合併進 metadata 區塊）

    Returns:
        最終寫出的檔案路徑
    """
    meta = {
        'schema_version': T3_SCHEMA_VERSION,
        'generated_at': datetime.now().isoformat(),
        'scanner': 'rs_leading_high',
        'total_signals': len(signals),
        'source': 'Stock001',
    }
    if extra_metadata:
        meta.update(extra_metadata)

    features = []
    for s in signals:
        feat = {
            'ticker': s.ticker,
            'asof_date': s.signal_date.strftime('%Y-%m-%d') if hasattr(s.signal_date, 'strftime') else str(s.signal_date),
            'rank': s.rank,
            'technical': {
                'rs_leading_high_score': s.quality_score,
                'rs_value': s.rs_value,
                'rs_lookback_high': s.rs_lookback_high,
                'days_since_rs_high': s.days_since_rs_high,
                'purple_dot_count': s.purple_dot_count_recent,
                'stock_price': s.stock_price,
                'distance_from_high_pct': s.stock_distance_from_high_pct,
                'rs_above_wma21': s.rs_above_wma21,
                'rs_long_term_trend_up': s.rs_long_term_trend_up,
                'volume_ratio': s.volume_ratio,
                'rs_slope_50d': s.rs_slope_50d,
                'above_sma200': s.above_sma200,
                'dollar_volume_50d': s.dollar_volume_50d,
            },
            'score_breakdown': s.score_breakdown,
            'themes': [s.theme] if s.theme else [],
            'passed_quality_filters': s.passed_quality_filters,
        }
        features.append(feat)

    payload = {'metadata': meta, 'features': features}
    os.makedirs(os.path.dirname(output_path) or '.', exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, default=str)
    return output_path


def validate_t3_schema(payload: Dict[str, Any]) -> List[str]:
    """驗證 T3 payload schema，回傳錯誤訊息清單（空 = 通過）"""
    errors = []
    if 'metadata' not in payload:
        errors.append('缺 metadata')
    if 'features' not in payload:
        errors.append('缺 features')
        return errors
    required_meta = ['schema_version', 'generated_at', 'scanner', 'total_signals']
    for k in required_meta:
        if k not in payload['metadata']:
            errors.append(f'metadata 缺 {k}')
    for i, feat in enumerate(payload['features']):
        for k in ['ticker', 'asof_date', 'technical']:
            if k not in feat:
                errors.append(f'features[{i}] 缺 {k}')
    return errors
