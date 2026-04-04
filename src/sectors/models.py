"""
行业分类数据模型
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List

# 从 storage 导入共享的 StockInfo
from storage.repository import StockInfo


# NASDAQ 11个标准板块定义
NASDAQ100_SECTORS: Dict[str, str] = {
    "Technology": "TECH",
    "Healthcare": "HEALTH",
    "Consumer Discretionary": "CONS_DISC",
    "Communication Services": "COMMS",
    "Consumer Staples": "CONS_STAP",
    "Energy": "ENERGY",
    "Financials": "FIN",
    "Industrials": "INDUST",
    "Materials": "MAT",
    "Real Estate": "REIT",
    "Utilities": "UTIL",
}

# sector_code -> sector_name 反转映射（用于快速查找）
SECTOR_CODE_TO_NAME: Dict[str, str] = {v: k for k, v in NASDAQ100_SECTORS.items()}


@dataclass(frozen=True)
class StockSector:
    """单只股票的行业属性"""
    symbol: str
    sector: str
    sector_code: str
    industry: str


@dataclass(frozen=True)
class SectorBreadthResult:
    """按行业聚合的宽度指标"""
    sector: str
    sector_code: str
    total_stocks: int
    above_200ma: int
    below_200ma: int
    breadth_pct: float
    leading_stocks: List[StockInfo] = field(default_factory=list)
    lagging_stocks: List[StockInfo] = field(default_factory=list)
