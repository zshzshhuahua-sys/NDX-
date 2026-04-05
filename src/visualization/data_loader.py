"""
数据加载器

从多个来源加载图表所需数据
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from logging import getLogger
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import yfinance as yf

from storage.repository import StockInfo
from sectors import FinnhubSectorProvider, SectorBreadthService, SectorSQLiteStorage


logger = getLogger(__name__)


@dataclass(frozen=True)
class BreadthDataPoint:
    """单日宽度数据"""
    trade_date: date
    breadth_pct: float
    valid_stocks: int
    above_200ma: int
    below_200ma: int


@dataclass(frozen=True)
class NDXDataPoint:
    """NDX 指数数据"""
    trade_date: date
    close: float


@dataclass(frozen=True)
class SectorDataPoint:
    """行业宽度数据"""
    sector_code: str
    sector_name: str
    breadth_pct: float
    above_200ma: int
    total_stocks: int


@dataclass(frozen=True)
class ChartData:
    """图表完整数据"""
    breadth_history: List[BreadthDataPoint] = field(default_factory=list)
    ndx_history: List[NDXDataPoint] = field(default_factory=list)
    sectors: List[SectorDataPoint] = field(default_factory=list)
    start_date: Optional[date] = None
    end_date: Optional[date] = None


class ChartDataLoader:
    """数据加载器"""

    def __init__(
        self,
        data_dir: Optional[Path] = None,
        ndx_symbol: str = "^NDX",
    ) -> None:
        self.data_dir = data_dir
        self.ndx_symbol = ndx_symbol

    def load(
        self,
        start_date: date,
        end_date: date,
        load_sectors: bool = True,
    ) -> ChartData:
        """
        加载图表所需全部数据

        Args:
            start_date: 起始日期
            end_date: 结束日期
            load_sectors: 是否加载行业数据

        Returns:
            ChartData 对象
        """
        # 加载宽度历史数据
        breadth_history = self._load_breadth_history(start_date, end_date)

        # 加载 NDX 指数数据
        ndx_history = self._load_ndx_history(start_date, end_date)

        # 加载行业数据（最新一天）
        sectors: List[SectorDataPoint] = []
        if load_sectors and breadth_history:
            sectors = self._load_sectors(breadth_history[-1].trade_date)

        return ChartData(
            breadth_history=breadth_history,
            ndx_history=ndx_history,
            sectors=sectors,
            start_date=start_date,
            end_date=end_date,
        )

    def _load_breadth_history(
        self,
        start_date: date,
        end_date: date,
    ) -> List[BreadthDataPoint]:
        """从 Parquet 加载宽度历史数据"""
        if self.data_dir is None:
            logger.warning("data_dir not set, using empty breadth history")
            return []

        parquet_path = self.data_dir / "breadth" / "history" / "breadth_history.parquet"

        if not parquet_path.exists():
            logger.warning("Breadth history file not found: %s", parquet_path)
            return []

        try:
            df = pd.read_parquet(parquet_path)

            # 确保 trade_date 是 datetime 类型
            df["trade_date"] = pd.to_datetime(df["trade_date"])

            df = df[
                (df["trade_date"] >= pd.to_datetime(start_date)) &
                (df["trade_date"] <= pd.to_datetime(end_date))
            ].sort_values("trade_date")

            return [
                BreadthDataPoint(
                    trade_date=pd.to_datetime(row["trade_date"]).date(),
                    breadth_pct=float(row["breadth_pct"]),
                    valid_stocks=int(row["valid_stocks"]),
                    above_200ma=int(row["above_200ma"]),
                    below_200ma=int(row["below_200ma"]),
                )
                for _, row in df.iterrows()
            ]
        except Exception as exc:
            logger.error("Failed to load breadth history: %s", exc)
            return []

    def _load_ndx_history(
        self,
        start_date: date,
        end_date: date,
    ) -> List[NDXDataPoint]:
        """从 yfinance 加载 NDX 指数数据"""
        try:
            ndx = yf.download(
                self.ndx_symbol,
                start=start_date,
                end=end_date + timedelta(days=1),
                auto_adjust=True,
                progress=False,
            )

            if ndx.empty:
                logger.warning("No NDX data returned")
                return []

            # 处理 MultiIndex 列 (新版本 yfinance)
            if isinstance(ndx.columns, pd.MultiIndex):
                # 尝试获取 Close 列
                if "Close" in ndx.columns.get_level_values(1):
                    closes = ndx.xs("Close", axis=1, level=1)
                elif "close" in ndx.columns.get_level_values(1):
                    closes = ndx.xs("close", axis=1, level=1)
                else:
                    # 使用第一个列作为 Close
                    closes = ndx.iloc[:, 0]
            else:
                # 普通 DataFrame
                cols = ndx.columns.tolist()
                close_names = ["Close", "close", "adj close", "Adj Close"]
                for name in close_names:
                    if name in cols:
                        closes = ndx[name]
                        break
                else:
                    closes = ndx.iloc[:, 0]

            return [
                NDXDataPoint(
                    trade_date=idx.date() if hasattr(idx, 'date') else idx,
                    close=float(close),
                )
                for idx, close in closes.items()
            ]
        except Exception as exc:
            logger.error("Failed to load NDX history: %s", exc)
            return []

    def _load_sectors(self, trade_date: date) -> List[SectorDataPoint]:
        """从每日 JSON 快照加载行业数据"""
        if self.data_dir is None:
            return []

        # 构造 JSON 路径
        date_str = trade_date.strftime("%Y-%m-%d")
        year = trade_date.strftime("%Y")
        month = trade_date.strftime("%m")
        json_path = (
            self.data_dir / "breadth" / "daily" / year / month / f"{date_str}.json"
        )

        if not json_path.exists():
            logger.warning("Daily snapshot not found: %s", json_path)
            return []

        try:
            with open(json_path) as f:
                data = json.load(f)

            # 重建 StockInfo 对象
            symbols_above = [StockInfo(**s) for s in data.get("symbols_above", [])]
            symbols_below = [StockInfo(**s) for s in data.get("symbols_below", [])]

            # 计算行业宽度
            data_dir = self.data_dir
            cache_dir = data_dir / "cache" / "sectors"
            sqlite_storage = SectorSQLiteStorage(db_path=data_dir / "sectors.db")
            sqlite_storage.initialize_sync()
            provider = FinnhubSectorProvider(
                cache_dir=cache_dir,
                sqlite_storage=sqlite_storage,
            )
            service = SectorBreadthService(provider)

            sector_results = service.calculate_sector_breadth(
                symbols_above=symbols_above,
                symbols_below=symbols_below,
            )

            return [
                SectorDataPoint(
                    sector_code=sr.sector_code,
                    sector_name=sr.sector,
                    breadth_pct=sr.breadth_pct,
                    above_200ma=sr.above_200ma,
                    total_stocks=sr.total_stocks,
                )
                for sr in sector_results.values()
                if sr.total_stocks > 0
            ]
        except Exception as exc:
            logger.error("Failed to load sectors: %s", exc)
            return []
