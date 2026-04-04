"""
行业宽度计算服务
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence

from .models import NASDAQ100_SECTORS, SectorBreadthResult, StockInfo, StockSector
from .provider import FinnhubSectorProvider


@dataclass(frozen=True)
class StockWithSector:
    """股票信息 + 行业属性"""
    stock: StockInfo
    sector: Optional[StockSector]


class SectorBreadthService:
    """行业宽度计算服务"""

    def __init__(
        self,
        sector_provider: FinnhubSectorProvider,
    ) -> None:
        self.provider = sector_provider

    def enrich_with_sectors(
        self,
        symbols_above: List[StockInfo],
        symbols_below: List[StockInfo],
    ) -> List[StockWithSector]:
        """为宽度数据中的每只股票添加行业属性"""
        results: List[StockWithSector] = []

        for stock in symbols_above:
            sector = self.provider.fetch(stock.symbol)
            results.append(StockWithSector(stock=stock, sector=sector))

        for stock in symbols_below:
            sector = self.provider.fetch(stock.symbol)
            results.append(StockWithSector(stock=stock, sector=sector))

        return results

    def calculate_sector_breadth(
        self,
        symbols_above: List[StockInfo],
        symbols_below: List[StockInfo],
    ) -> Dict[str, SectorBreadthResult]:
        """
        按行业聚合计算宽度指标

        Args:
            symbols_above: 在均线上方的股票列表
            symbols_below: 在均线下方股票列表

        Returns:
            sector_code -> SectorBreadthResult 的映射
        """
        # 按行业分组
        sector_stocks: Dict[str, Dict[str, List[StockInfo]]] = {
            sc: {"above": [], "below": []}
            for sc in NASDAQ100_SECTORS.values()
        }
        sector_stocks["OTHER"] = {"above": [], "below": []}

        # 遍历所有股票，按行业和均线上/下分组
        for stock in symbols_above:
            sector = self.provider.fetch(stock.symbol)
            sc = sector.sector_code if sector else "OTHER"
            if sc not in sector_stocks:
                sector_stocks[sc] = {"above": [], "below": []}
            sector_stocks[sc]["above"].append(stock)

        for stock in symbols_below:
            sector = self.provider.fetch(stock.symbol)
            sc = sector.sector_code if sector else "OTHER"
            if sc not in sector_stocks:
                sector_stocks[sc] = {"above": [], "below": []}
            sector_stocks[sc]["below"].append(stock)

        # 计算各行业宽度
        results: Dict[str, SectorBreadthResult] = {}

        for sc, stocks_dict in sector_stocks.items():
            above_list = stocks_dict["above"]
            below_list = stocks_dict["below"]
            total = len(above_list) + len(below_list)

            if total == 0:
                continue

            # 领先股（偏离度最大的）
            leading = sorted(
                above_list,
                key=lambda s: s.deviation,
                reverse=True,
            )[:3]

            # 落后股（偏离度最小的）
            lagging = sorted(
                below_list,
                key=lambda s: s.deviation,
            )[:3]

            # 获取板块名称
            sector_name = sc
            for name, code in NASDAQ100_SECTORS.items():
                if code == sc:
                    sector_name = name
                    break

            results[sc] = SectorBreadthResult(
                sector=sector_name,
                sector_code=sc,
                total_stocks=total,
                above_200ma=len(above_list),
                below_200ma=len(below_list),
                breadth_pct=round(len(above_list) / total * 100, 2),
                leading_stocks=leading,
                lagging_stocks=lagging,
            )

        return results
