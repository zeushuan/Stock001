"""族群映射表載入與管理"""
import os
from typing import List, Dict, Any, Optional

import yaml


DEFAULT_CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    'config', 'peer_groups.yaml'
)


class PeerMapping:
    """族群映射表

    支援：
      - 從 yaml 載入
      - 查詢某 ticker 屬於哪些 group
      - 列出所有 group 與成員
      - ticker 格式驗證
    """

    def __init__(self, config_path: str = DEFAULT_CONFIG_PATH):
        self.config_path = config_path
        with open(config_path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
        self.groups: Dict[str, Dict[str, Any]] = data.get('groups', {}) or {}
        self.settings: Dict[str, Any] = data.get('settings', {}) or {}
        self._validate()

    def _validate(self):
        """驗證 yaml 內容合理性"""
        for gname, g in self.groups.items():
            members = g.get('members', [])
            if not members:
                raise ValueError(f'group "{gname}" 無 members')
            for m in members:
                if not isinstance(m, str):
                    raise ValueError(f'group "{gname}" member "{m}" 必須是 str')
            # leader_candidates 必須是 members 子集
            for lc in g.get('leader_candidates', []):
                if lc not in members:
                    raise ValueError(
                        f'group "{gname}" leader_candidate "{lc}" 不在 members 中')

    def list_groups(self) -> List[str]:
        return list(self.groups.keys())

    def get_group(self, ticker: str) -> List[str]:
        """回傳 ticker 所屬的所有 group 名稱（一檔可同時屬於多 group）"""
        return [name for name, g in self.groups.items()
                if ticker in g.get('members', [])]

    def get_members(self, group_name: str) -> List[str]:
        g = self.groups.get(group_name)
        if g is None:
            raise KeyError(f'group "{group_name}" 不存在')
        return list(g.get('members', []))

    def get_leader_candidates(self, group_name: str) -> List[str]:
        g = self.groups.get(group_name)
        return list((g or {}).get('leader_candidates', []))

    def get_description(self, group_name: str) -> str:
        g = self.groups.get(group_name) or {}
        return g.get('description', '')

    def market_of_group(self, group_name: str) -> str:
        """猜測該 group 主要是哪個市場（依 members 是否含 .TW）"""
        members = self.get_members(group_name)
        tw = sum(1 for m in members if '.TW' in m)
        return 'TW' if tw > len(members) / 2 else 'US'

    def get_setting(self, key: str, default=None):
        return self.settings.get(key, default)


_default_mapping: Optional[PeerMapping] = None


def get_default_mapping() -> PeerMapping:
    """singleton 載入，避免重複讀檔"""
    global _default_mapping
    if _default_mapping is None:
        _default_mapping = PeerMapping()
    return _default_mapping
