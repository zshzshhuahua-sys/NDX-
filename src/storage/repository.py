"""
Breadth Repository 接口定义
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import List, Optional


@dataclass(frozen=True)
class StockInfo:
    """单只股票信息"""
    symbol: str
    close: float
    sma200: float
    deviation: float


@dataclass(frozen=True)
class InvalidStock:
    """无效股票信息"""
    symbol: str
    reason: str


@dataclass(frozen=True)
class BreadthRecord:
    """单条宽度记录"""
    trade_date: date
    breadth_pct: float
    valid_stocks: int
    above_200ma: int
    below_200ma: int
    invalid_stocks: int
    calculated_at: Optional[datetime] = None


@dataclass(frozen=True)
class BreadthDetail(BreadthRecord):
    """带详情的宽度记录"""
    symbols_above: List[StockInfo] = field(default_factory=list)
    symbols_below: List[StockInfo] = field(default_factory=list)
    symbols_invalid: List[InvalidStock] = field(default_factory=list)
    constituents_source: Optional[str] = None
    data_source: Optional[str] = None


class BreadthRepository(ABC):
    """宽度数据仓库接口"""

    @abstractmethod
    def save(
        self,
        trade_date: str,
        breadth_pct: float,
        valid_stocks: int,
        above_200ma: int,
        below_200ma: int,
        invalid_stocks: int,
        symbols_above: List[dict],
        symbols_below: List[dict],
        symbols_invalid: List[dict],
        constituents_source: str = "unknown",
        data_source: str = "yfinance",
    ) -> None:
        """保存单日计算结果"""
        pass

    @abstractmethod
    def find_by_date(self, trade_date: date) -> Optional[BreadthDetail]:
        """查询指定日期数据"""
        pass

    @abstractmethod
    def find_range(
        self,
        start_date: date,
        end_date: date,
    ) -> List[BreadthRecord]:
        """查询日期范围内的数据"""
        pass

    @abstractmethod
    def find_latest(self) -> Optional[BreadthDetail]:
        """获取最新数据"""
        pass
