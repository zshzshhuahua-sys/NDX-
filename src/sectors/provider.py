"""
Finnhub 行业数据获取器

提供 NASDAQ-100 成分股的行业分类数据
"""

from __future__ import annotations

import fcntl
import json
import logging
import os
import time
from pathlib import Path
from typing import Dict, List, Optional

import requests

from .fallback import NASDAQ100_SECTOR_MAPPING, get_fallback_sector
from .models import NASDAQ100_SECTORS, StockSector


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s"
)
logger = logging.getLogger(__name__)


class FinnhubSectorProvider:
    """从 Finnhub API 获取行业分类"""

    BASE_URL = "https://finnhub.io/api/v1/stock/profile2"

    def __init__(
        self,
        api_key: Optional[str] = None,
        cache_dir: Optional[Path] = None,
        rate_limit: float = 1.0,
    ) -> None:
        self.api_key = api_key or os.environ.get("FINNHUB_API_KEY")
        self.cache_dir = cache_dir
        self.rate_limit = rate_limit
        self.session = requests.Session()
        self._memory_cache: Dict[str, StockSector] = {}
        self._last_request_time: float = 0
        self._logged_fallback_warning: bool = False

    def fetch(self, symbol: str) -> Optional[StockSector]:
        """
        获取单只股票的行业分类

        Args:
            symbol: 股票代码

        Returns:
            StockSector 或 None（如果获取失败）
        """
        # 1. 先查内存缓存
        if symbol in self._memory_cache:
            return self._memory_cache[symbol]

        # 2. 查本地缓存
        cached = self._load_from_cache(symbol)
        if cached:
            self._memory_cache[symbol] = cached
            return cached

        # 3. 如果没有 API Key，使用 fallback 硬编码映射
        if not self.api_key:
            if not self._logged_fallback_warning:
                logger.info("FINNHUB_API_KEY not set, using fallback sector mapping")
                self._logged_fallback_warning = True
            fallback_data = get_fallback_sector(symbol)
            sector = StockSector(
                symbol=symbol,
                sector=fallback_data["sector"],
                sector_code=fallback_data["sector_code"],
                industry=fallback_data["industry"],
            )
            self._memory_cache[symbol] = sector
            return sector

        sector = self._request_from_api(symbol)
        if sector:
            self._memory_cache[symbol] = sector
            self._save_to_cache(sector)

        return sector

    def fetch_batch(self, symbols: List[str]) -> Dict[str, StockSector]:
        """
        批量获取行业分类（带速率限制）

        Args:
            symbols: 股票代码列表

        Returns:
            symbol -> StockSector 的映射
        """
        results: Dict[str, StockSector] = {}
        fetched: List[str] = []

        for symbol in symbols:
            sector = self.fetch(symbol)
            if sector:
                results[symbol] = sector
                fetched.append(symbol)
            else:
                logger.warning(f"Failed to fetch sector for {symbol}")

            # 速率限制
            self._wait_for_rate_limit()

        logger.info(f"Fetched sectors for {len(fetched)}/{len(symbols)} symbols")
        return results

    def _request_from_api(self, symbol: str) -> Optional[StockSector]:
        """从 Finnhub API 请求数据"""
        url = f"{self.BASE_URL}"
        params = {"symbol": symbol, "token": self.api_key}

        try:
            response = self.session.get(url, params=params, timeout=20)
            response.raise_for_status()
            data = response.json()

            # Finnhub 返回 finnhubIndustry 字段
            sector_name = data.get("finnhubIndustry") or "Unknown"
            sector_code = self._normalize_sector_code(sector_name)
            industry = data.get("industry") or "Unknown"

            return StockSector(
                symbol=symbol,
                sector=sector_name,
                sector_code=sector_code,
                industry=industry,
            )

        except requests.exceptions.RequestException as exc:
            logger.warning(f"Finnhub API error for {symbol}: {exc}")
            return None
        except (KeyError, ValueError) as exc:
            logger.warning(f"Parse error for {symbol}: {exc}")
            return None

    def _normalize_sector_code(self, sector_name: str) -> str:
        """将 Finnhub 板块名称转换为标准代码"""
        # 先尝试直接匹配
        if sector_name in NASDAQ100_SECTORS:
            return NASDAQ100_SECTORS[sector_name]

        # Finnhub 行业名称映射到标准板块
        FINNHUB_SECTOR_MAP: Dict[str, str] = {
            # Technology
            "Technology": "TECH",
            "Semiconductors": "TECH",
            "Software": "TECH",
            "Hardware": "TECH",
            "IT Services": "TECH",
            "Computer Hardware": "TECH",
            "Electronic Components": "TECH",
            "Semiconductor Equipment": "TECH",
            # Healthcare
            "Healthcare": "HEALTH",
            "Pharmaceuticals": "HEALTH",
            "Biotechnology": "HEALTH",
            "Medical Devices": "HEALTH",
            "Diagnostics": "HEALTH",
            "Healthcare Plans": "HEALTH",
            # Consumer Discretionary
            "Consumer Discretionary": "CONS_DISC",
            "Retail": "CONS_DISC",
            "Automobiles": "CONS_DISC",
            "Media": "COMMS",  # Finnhub's "Media" maps to COMMS
            "Travel": "CONS_DISC",
            "Travel Services": "CONS_DISC",
            "Hotels": "CONS_DISC",
            "Restaurants": "CONS_DISC",
            "Apparel": "CONS_DISC",
            "Internet Retail": "CONS_DISC",
            # Communication Services
            "Communication Services": "COMMS",
            "Telecom": "COMMS",
            "Social Media": "COMMS",
            "Entertainment": "COMMS",
            # Consumer Staples
            "Consumer Staples": "CONS_STAP",
            "Beverages": "CONS_STAP",
            "Food": "CONS_STAP",
            "Tobacco": "CONS_STAP",
            "Household": "CONS_STAP",
            # Energy
            "Energy": "ENERGY",
            "Oil & Gas": "ENERGY",
            "Oil & Gas Exploration": "ENERGY",
            "Oil & Gas Services": "ENERGY",
            # Financials
            "Financials": "FIN",
            "Banking": "FIN",
            "Financial Services": "FIN",
            "Insurance": "FIN",
            "Asset Management": "FIN",
            "Investment Banking": "FIN",
            "Payments": "FIN",
            # Industrials
            "Industrials": "INDUST",
            "Aerospace": "INDUST",
            "Defense": "INDUST",
            "Heavy Machinery": "INDUST",
            "Electrical Equipment": "INDUST",
            "Conglomerates": "INDUST",
            "Railroads": "INDUST",
            "Trucking": "INDUST",
            "Airlines": "INDUST",
            "Services": "INDUST",
            # Materials
            "Materials": "MAT",
            "Chemicals": "MAT",
            "Mining": "MAT",
            "Metals": "MAT",
            "Paper": "MAT",
            # Real Estate
            "Real Estate": "REIT",
            "REIT": "REIT",
            "Real Estate Services": "REIT",
            # Utilities
            "Utilities": "UTIL",
        }

        return FINNHUB_SECTOR_MAP.get(sector_name, "OTHER")

    def _wait_for_rate_limit(self) -> None:
        """速率限制：确保请求间隔不小于 rate_limit 秒"""
        elapsed = time.time() - self._last_request_time
        if elapsed < self.rate_limit:
            time.sleep(self.rate_limit - elapsed)
        self._last_request_time = time.time()

    def _cache_path(self, symbol: str) -> Path:
        """获取缓存文件路径"""
        if self.cache_dir is None:
            return Path("/dev/null")
        return self.cache_dir / f"{symbol}.json"

    def _load_from_cache(self, symbol: str) -> Optional[StockSector]:
        """从本地缓存加载"""
        if self.cache_dir is None:
            return None

        path = self._cache_path(symbol)
        if not path.exists():
            return None

        try:
            with open(path, "r") as f:
                fcntl.flock(f.fileno(), fcntl.LOCK_SH)
                data = json.load(f)
            return StockSector(**data)
        except (json.JSONDecodeError, TypeError) as exc:
            logger.warning(f"Cache read error for {symbol}: {exc}")
            return None

    def _save_to_cache(self, sector: StockSector) -> None:
        """保存到本地缓存"""
        if self.cache_dir is None:
            return

        self.cache_dir.mkdir(parents=True, exist_ok=True)
        path = self._cache_path(sector.symbol)

        try:
            with open(path, "w") as f:
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                json.dump(
                    {
                        "symbol": sector.symbol,
                        "sector": sector.sector,
                        "sector_code": sector.sector_code,
                        "industry": sector.industry,
                    },
                    f,
                    indent=2,
                )
        except OSError as exc:
            logger.warning(f"Cache write error for {sector.symbol}: {exc}")


def get_finnhub_provider(
    api_key: Optional[str] = None,
    cache_dir: Optional[Path] = None,
) -> FinnhubSectorProvider:
    """便捷函数：创建 FinnhubSectorProvider"""
    return FinnhubSectorProvider(api_key=api_key, cache_dir=cache_dir)
