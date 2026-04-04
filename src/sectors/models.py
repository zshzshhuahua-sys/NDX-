"""
行业分类数据模型
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional


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


@dataclass(frozen=True)
class StockSector:
    """单只股票的行业属性"""
    symbol: str
    sector: str
    sector_code: str
    industry: str


@dataclass(frozen=True)
class StockInfo:
    """股票价格信息（用于行业宽度计算）"""
    symbol: str
    close: float
    sma200: float
    deviation: float


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
