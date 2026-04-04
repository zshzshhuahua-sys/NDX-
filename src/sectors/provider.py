"""
Finnhub 行业数据获取器

提供 NASDAQ-100 成分股的行业分类数据
支持异步批量获取，优化 API 调用效率
集成 SQLite 本地存储
"""

from __future__ import annotations

import asyncio
import fcntl
import json
import logging
import os
import time
from pathlib import Path
from typing import Dict, List, Optional, Set

import aiohttp
import requests

from .fallback import NASDAQ100_SECTOR_MAPPING, get_fallback_sector
from .models import NASDAQ100_SECTORS, StockSector
from .storage import SectorSQLiteStorage


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s"
)
logger = logging.getLogger(__name__)

# 并发控制：最多同时 5 个请求
MAX_CONCURRENT_REQUESTS = 5
# 速率限制：每次请求间隔（秒）
RATE_LIMIT_SECONDS = 0.5


class FinnhubSectorProvider:
    """从 Finnhub API 获取行业分类（同步 + 异步接口）"""

    BASE_URL = "https://finnhub.io/api/v1/stock/profile2"

    def __init__(
        self,
        api_key: Optional[str] = None,
        cache_dir: Optional[Path] = None,
        rate_limit: float = RATE_LIMIT_SECONDS,
        sqlite_storage: Optional[SectorSQLiteStorage] = None,
    ) -> None:
        self.api_key = api_key or os.environ.get("FINNHUB_API_KEY")
        self.cache_dir = cache_dir
        self.rate_limit = rate_limit
        self.sqlite_storage = sqlite_storage
        self.session = requests.Session()
        self._memory_cache: Dict[str, StockSector] = {}
        self._last_request_time: float = 0
        self._logged_fallback_warning: bool = False
        self._logged_api_warning: bool = False

    def fetch(self, symbol: str) -> Optional[StockSector]:
        """
        获取单只股票的行业分类（同步接口）

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

        # 3. 查询 fallback 硬编码映射
        fallback_data = get_fallback_sector(symbol)
        if fallback_data["sector"] != "Unknown":
            sector = StockSector(
                symbol=symbol,
                sector=fallback_data["sector"],
                sector_code=fallback_data["sector_code"],
                industry=fallback_data["industry"],
            )
            self._memory_cache[symbol] = sector
            return sector

        # 4. 如果没有 API Key，返回 None
        if not self.api_key:
            if not self._logged_fallback_warning:
                logger.warning("FINNHUB_API_KEY not set, using fallback only")
                self._logged_fallback_warning = True
            return None

        # 5. 请求 Finnhub API
        sector = self._request_from_api(symbol)
        if sector:
            self._memory_cache[symbol] = sector
            self._save_to_cache(sector)

        return sector

    def fetch_batch(self, symbols: List[str]) -> Dict[str, Optional[StockSector]]:
        """
        批量获取行业分类（同步接口，内部使用异步实现）

        Args:
            symbols: 股票代码列表

        Returns:
            symbol -> StockSector 的映射（未找到的返回 None）
        """
        # 去重
        unique_symbols = list(set(symbols))

        # 收集已缓存的结果
        results: Dict[str, Optional[StockSector]] = {}
        to_fetch: List[str] = []
        sqlite_cached: List[str] = []

        for symbol in unique_symbols:
            # 1. 检查内存缓存
            if symbol in self._memory_cache:
                results[symbol] = self._memory_cache[symbol]
                continue

            # 2. 检查本地 JSON 缓存
            cached = self._load_from_cache(symbol)
            if cached:
                self._memory_cache[symbol] = cached
                results[symbol] = cached
                continue

            # 3. 检查 fallback
            fallback_data = get_fallback_sector(symbol)
            if fallback_data["sector"] != "Unknown":
                sector = StockSector(
                    symbol=symbol,
                    sector=fallback_data["sector"],
                    sector_code=fallback_data["sector_code"],
                    industry=fallback_data["industry"],
                )
                self._memory_cache[symbol] = sector
                results[symbol] = sector
                # 保存到 SQLite
                if self.sqlite_storage:
                    sqlite_cached.append(symbol)
                continue

            # 4. 如果配置了 SQLite 存储，检查 SQLite
            if self.sqlite_storage:
                sqlite_cached.append(symbol)
                results[symbol] = None  # 暂时设为 None，稍后从 SQLite 获取
            else:
                # 没有 SQLite，需要 API 请求
                to_fetch.append(symbol)
                results[symbol] = None

        # 从 SQLite 批量获取
        if sqlite_cached and self.sqlite_storage:
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                sqlite_results = loop.run_until_complete(
                    self.sqlite_storage.get_batch(sqlite_cached)
                )
                loop.close()

                for symbol, record in sqlite_results.items():
                    if record is not None:
                        sector = record.to_stock_sector()
                        self._memory_cache[symbol] = sector
                        results[symbol] = sector
                    else:
                        # SQLite 中也没有，需要 API 请求
                        to_fetch.append(symbol)
            except Exception as exc:
                logger.warning(f"SQLite query failed: {exc}")
                # SQLite 查询失败，尝试 API 请求
                to_fetch.extend(sqlite_cached)

        # 如果有 API 请求，使用异步批量获取
        if to_fetch and self.api_key:
            try:
                # 创建新的事件循环
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                api_results = loop.run_until_complete(
                    self._fetch_batch_async(to_fetch)
                )
                loop.close()

                # 合并结果
                for symbol, sector in api_results.items():
                    results[symbol] = sector
                    if sector:
                        self._memory_cache[symbol] = sector
                        self._save_to_cache(sector)
            except Exception as exc:
                logger.error(f"Async batch fetch failed: {exc}")
                # Fallback 到串行请求
                for symbol in to_fetch:
                    if results[symbol] is None:
                        sector = self._request_from_api(symbol)
                        results[symbol] = sector
                        if sector:
                            self._memory_cache[symbol] = sector
                            self._save_to_cache(sector)
                        self._wait_for_rate_limit()

        logger.info(f"Fetched sectors: {len([r for r in results.values() if r])}/{len(unique_symbols)} from cache/fallback, {len(to_fetch) - len([r for r in results.values() if r and r.symbol in to_fetch])}/{len(to_fetch)} from API")
        return results

    async def _fetch_batch_async(self, symbols: List[str]) -> Dict[str, Optional[StockSector]]:
        """
        异步批量获取行业分类（真正的并发请求）

        Args:
            symbols: 股票代码列表

        Returns:
            symbol -> StockSector 的映射
        """
        results: Dict[str, Optional[StockSector]] = {s: None for s in symbols}

        # 使用信号量控制并发数
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

        async def fetch_with_semaphore(symbol: str) -> tuple[str, Optional[StockSector]]:
            async with semaphore:
                return symbol, await self._fetch_async(symbol)

        # 并发执行所有请求
        tasks = [fetch_with_semaphore(symbol) for symbol in symbols]
        completed = await asyncio.gather(*tasks, return_exceptions=True)

        # 收集结果
        for item in completed:
            if isinstance(item, tuple):
                symbol, sector = item
                results[symbol] = sector
            elif isinstance(item, Exception):
                logger.warning(f"Fetch error: {item}")

        return results

    async def _fetch_async(self, symbol: str) -> Optional[StockSector]:
        """异步请求单只股票的行业分类"""
        if not self.api_key:
            return None

        url = f"{self.BASE_URL}"
        params = {"symbol": symbol, "token": self.api_key}

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, timeout=20) as response:
                    if response.status == 429:
                        logger.warning(f"Finnhub rate limited for {symbol}, skipping...")
                        return None

                    response.raise_for_status()
                    data = await response.json()

                    sector_name = data.get("finnhubIndustry") or "Unknown"
                    sector_code = self._normalize_sector_code(sector_name)
                    industry = data.get("industry") or "Unknown"

                    return StockSector(
                        symbol=symbol,
                        sector=sector_name,
                        sector_code=sector_code,
                        industry=industry,
                    )

        except asyncio.TimeoutError:
            logger.warning(f"Timeout fetching {symbol}")
            return None
        except aiohttp.ClientError as exc:
            logger.warning(f"HTTP error for {symbol}: {exc}")
            return None
        except (KeyError, ValueError) as exc:
            logger.warning(f"Parse error for {symbol}: {exc}")
            return None

    def _request_from_api(self, symbol: str) -> Optional[StockSector]:
        """从 Finnhub API 请求数据（同步接口）"""
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
            if not self._logged_api_warning:
                logger.warning(f"Finnhub API error for {symbol}: {exc}")
                self._logged_api_warning = True
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

    def clear_memory_cache(self) -> None:
        """清空内存缓存"""
        self._memory_cache.clear()
        logger.debug("Memory cache cleared")

    def get_cache_stats(self) -> Dict[str, int]:
        """获取缓存统计信息"""
        return {
            "memory_cache_size": len(self._memory_cache),
            "cache_dir": str(self.cache_dir) if self.cache_dir else "None",
        }


def get_finnhub_provider(
    api_key: Optional[str] = None,
    cache_dir: Optional[Path] = None,
) -> FinnhubSectorProvider:
    """便捷函数：创建 FinnhubSectorProvider"""
    return FinnhubSectorProvider(api_key=api_key, cache_dir=cache_dir)
