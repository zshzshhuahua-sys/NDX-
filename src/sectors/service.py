"""
行业宽度计算服务
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence

from .models import NASDAQ100_SECTORS, SECTOR_CODE_TO_NAME, SectorBreadthResult, StockInfo, StockSector
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
        # 1. 合并并去重所有股票代码，避免重复 API 调用
        all_stocks = symbols_above + symbols_below
        unique_symbols = list({s.symbol for s in all_stocks})

        # 2. 批量预获取所有唯一股票的行业分类
        sector_map = self.provider.fetch_batch(unique_symbols)

        # 3. 按行业分组
        sector_stocks: Dict[str, Dict[str, List[StockInfo]]] = {
            sc: {"above": [], "below": []}
            for sc in NASDAQ100_SECTORS.values()
        }
        sector_stocks["OTHER"] = {"above": [], "below": []}

        # 4. 使用预获取的数据分组（无重复 API 调用）
        for stock in symbols_above:
            sector = sector_map.get(stock.symbol)
            sc = sector.sector_code if sector else "OTHER"
            if sc not in sector_stocks:
                sector_stocks[sc] = {"above": [], "below": []}
            sector_stocks[sc]["above"].append(stock)

        for stock in symbols_below:
            sector = sector_map.get(stock.symbol)
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

            # 获取板块名称（使用反转映射 O(1) 查找）
            sector_name = SECTOR_CODE_TO_NAME.get(sc, sc)

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
