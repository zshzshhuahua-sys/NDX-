"""
NASDAQ-100 成分股动态获取模块

支持多数据源优先级、缓存、重试机制

数据源优先级:
1. Wikipedia (默认)
2. Yahoo Finance (备用)
3. 硬编码列表 (最终兜底)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import List, Optional, Tuple
import json
import logging
import time

import pandas as pd
import requests


# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s"
)
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ConstituentsSnapshot:
    """成分股快照"""
    as_of_date: str
    fetched_at: str
    source: str
    symbols: List[str]
    symbol_count: int = field(init=False)

    def __post_init__(self):
        # frozen dataclass 需要用 object.__setattr__
        object.__setattr__(self, 'symbol_count', len(self.symbols))


# 硬编码的 NASDAQ-100 成分股 (最终兜底)
# 注意: PEAK 和 WADM 已退市，已移除
HARDCODE_NASDAQ_100: List[str] = [
    "AAPL", "ABNB", "ADBE", "ADI", "ADP", "ADSK", "AEP", "AMAT", "AMGN", "AMZN",
    "ANET", "ARM", "AVGO", "AXON", "BKNG", "BKR", "CDNS", "CDW", "CEG", "CHTR",
    "CMCSA", "COIN", "COST", "CPRT", "CRWD", "CSX", "CTAS", "CTSH", "DASH", "DDOG",
    "DLTR", "DXCM", "EA", "EXC", "FANG", "FAST", "FTNT", "GEHC", "GFS", "GILD",
    "GOOG", "GOOGL", "HON", "IDXX", "INTC", "INTU", "ISRG", "JBL", "JD", "KLAC",
    "KDP", "KHC", "LCTC", "LIN", "LRCX", "LU", "MAR", "MCHP", "MDLZ", "META",
    "MNST", "MOH", "MRNA", "MRVL", "MSFT", "MU", "NFLX", "NVDA", "NXPI", "ODFL",
    "ON", "ORLY", "PANW", "PAYX", "PCAR", "PDD", "PLTR", "PM", "PTC",
    "PYPL", "QCOM", "REGN", "ROP", "ROST", "SBUX", "SMCI", "SNPS", "TEAM", "TMUS",
    "TTD", "TTWO", "TXN", "UAL", "UBER", "ULTA", "VEEV", "VRTX", "WDAY",
    "WDC", "WEC", "XEL", "ZS"
]


class ConstituentsFetcher:
    """NASDAQ-100 成分股获取器"""

    def __init__(
        self,
        cache_dir: Optional[Path] = None,
        timeout_seconds: int = 20,
        max_retries: int = 3,
        retry_backoff_seconds: float = 1.5,
    ) -> None:
        self.cache_dir = cache_dir
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.retry_backoff_seconds = retry_backoff_seconds
        self.session = requests.Session()

    def get_constituents(
        self,
        as_of_date: Optional[date] = None,
        use_cache: bool = True,
    ) -> List[str]:
        """
        获取 NASDAQ-100 成分股列表

        Args:
            as_of_date: 目标日期 (None表示今天)
            use_cache: 是否使用缓存

        Returns:
            股票代码列表
        """
        target_date = as_of_date or date.today()

        # 1. 尝试从缓存加载
        if use_cache:
            cached = self._load_snapshot(target_date)
            if cached is not None:
                logger.info(
                    f"Using cached constituents from {cached.source} "
                    f"({cached.symbol_count} symbols)"
                )
                return cached.symbols

        # 2. 按优先级尝试各数据源
        providers: List[Tuple[str, callable]] = [
            ("nasdaq_api", self._fetch_from_nasdaq_api),
            ("wikipedia", self._fetch_from_wikipedia),
            ("yahoo_finance", self._fetch_from_yahoo_finance),
        ]

        last_error: Optional[Exception] = None

        for source_name, provider in providers:
            try:
                symbols = self._fetch_with_retry(provider, source_name)
                normalized = self._normalize_symbols(symbols)
                self._validate_symbols(normalized)

                # 保存快照
                snapshot = ConstituentsSnapshot(
                    as_of_date=target_date.isoformat(),
                    fetched_at=datetime.utcnow().isoformat(timespec="seconds") + "Z",
                    source=source_name,
                    symbols=normalized,
                )
                self._save_snapshot(snapshot)

                logger.info(
                    f"Fetched {snapshot.symbol_count} constituents from {source_name}"
                )
                return snapshot.symbols

            except Exception as exc:
                logger.warning(f"Failed to fetch from {source_name}: {exc}")
                last_error = exc

        # 3. 所有数据源都失败，尝试使用最新缓存
        latest = self._load_latest_snapshot()
        if latest is not None:
            logger.warning(
                f"All sources failed, using latest cached snapshot "
                f"from {latest.as_of_date}"
            )
            return latest.symbols

        # 4. 最终兜底：使用硬编码列表
        logger.warning("All sources failed, using hardcoded fallback list")
        return HARDCODE_NASDAQ_100.copy()

    def _fetch_with_retry(
        self,
        provider: callable,
        source_name: str,
    ) -> List[str]:
        """带重试的获取"""
        last_error: Optional[Exception] = None

        for attempt in range(1, self.max_retries + 1):
            try:
                return provider()
            except Exception as exc:
                last_error = exc
                if attempt == self.max_retries:
                    break
                sleep_seconds = self.retry_backoff_seconds * attempt
                logger.info(
                    f"Retrying {source_name} in {sleep_seconds:.1f}s "
                    f"(attempt {attempt + 1}/{self.max_retries})"
                )
                time.sleep(sleep_seconds)

        raise RuntimeError(f"{source_name} failed after {self.max_retries} retries") from last_error

    def _fetch_from_nasdaq_api(self) -> List[str]:
        """从 NASDAQ 官方 API 获取成分股（无需 API Key）"""
        logger.info("Fetching from NASDAQ official API...")

        url = "https://api.nasdaq.com/api/quote/list-type/nasdaq100"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/91.0.4472.124 Safari/537.36"
        }

        response = self.session.get(url, headers=headers, timeout=self.timeout_seconds)
        response.raise_for_status()

        data = response.json()
        rows = data.get("data", {}).get("data", {}).get("rows", [])

        symbols = [stock["symbol"] for stock in rows if stock.get("symbol")]
        logger.info(f"Found {len(symbols)} symbols from NASDAQ API")
        return symbols

    def _fetch_from_wikipedia(self) -> List[str]:
        """从 Wikipedia 获取成分股"""
        logger.info("Fetching from Wikipedia...")

        url = "https://en.wikipedia.org/wiki/NASDAQ-100"
        tables = pd.read_html(url)

        for table in tables:
            columns = {str(col).strip().lower() for col in table.columns}
            if "ticker" in columns:
                ticker_col = next(
                    col for col in table.columns
                    if str(col).strip().lower() == "ticker"
                )
                symbols = table[ticker_col].dropna().astype(str).tolist()
                logger.info(f"Found {len(symbols)} symbols from Wikipedia table")
                return symbols

        raise ValueError("NASDAQ-100 table not found on Wikipedia page")

    def _fetch_from_yahoo_finance(self) -> List[str]:
        """从 Yahoo Finance 获取成分股"""
        logger.info("Fetching from Yahoo Finance...")

        url = "https://finance.yahoo.com/quote/%5ENDX/components/"
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36"
        }

        response = self.session.get(url, headers=headers, timeout=self.timeout_seconds)
        response.raise_for_status()

        # 简单解析 - 实际可能需要更复杂的 HTML 解析
        from io import StringIO
        tables = pd.read_html(StringIO(response.text))

        for table in tables:
            if "Symbol" in table.columns or "Ticker" in table.columns:
                col = "Symbol" if "Symbol" in table.columns else "Ticker"
                symbols = table[col].dropna().astype(str).tolist()
                logger.info(f"Found {len(symbols)} symbols from Yahoo Finance")
                return symbols

        raise ValueError("NASDAQ-100 components not found on Yahoo Finance page")

    def _normalize_symbols(self, symbols: List[str]) -> List[str]:
        """标准化股票代码"""
        normalized = []
        for symbol in symbols:
            # 清理并转大写
            cleaned = symbol.strip().upper()
            # 转换可能的格式问题
            cleaned = cleaned.replace(".", "-")
            # 移除空格
            cleaned = cleaned.replace(" ", "")
            if cleaned and len(cleaned) <= 5:
                normalized.append(cleaned)

        # 去重并排序
        return sorted(set(normalized))

    def _validate_symbols(self, symbols: List[str]) -> None:
        """验证股票代码"""
        # 数量检查
        if not 95 <= len(symbols) <= 110:
            raise ValueError(f"Unexpected symbol count: {len(symbols)} (expected 95-110)")

        # 必须包含主要股票
        required = {"AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META"}
        missing = required.difference(symbols)
        if missing:
            raise ValueError(f"Missing required symbols: {sorted(missing)}")

        logger.debug(f"Validation passed: {len(symbols)} symbols")

    def _snapshot_path(self, as_of_date: date) -> Path:
        """获取快照文件路径"""
        if self.cache_dir is None:
            return Path("/dev/null")
        return self.cache_dir / f"{as_of_date.isoformat()}.json"

    def _load_snapshot(self, as_of_date: date) -> Optional[ConstituentsSnapshot]:
        """加载指定日期的快照"""
        if self.cache_dir is None:
            return None

        path = self._snapshot_path(as_of_date)
        if not path.exists():
            return None

        try:
            payload = json.loads(path.read_text())
            return ConstituentsSnapshot(**payload)
        except Exception as exc:
            logger.warning(f"Failed to load snapshot {path}: {exc}")
            return None

    def _load_latest_snapshot(self) -> Optional[ConstituentsSnapshot]:
        """加载最新的快照"""
        if self.cache_dir is None:
            return None

        latest_path = self.cache_dir / "latest.json"
        if not latest_path.exists():
            return None

        try:
            payload = json.loads(latest_path.read_text())
            return ConstituentsSnapshot(**payload)
        except Exception as exc:
            logger.warning(f"Failed to load latest snapshot: {exc}")
            return None

    def _save_snapshot(self, snapshot: ConstituentsSnapshot) -> None:
        """保存快照"""
        if self.cache_dir is None:
            return

        self.cache_dir.mkdir(parents=True, exist_ok=True)

        payload = {
            "as_of_date": snapshot.as_of_date,
            "fetched_at": snapshot.fetched_at,
            "source": snapshot.source,
            "symbols": snapshot.symbols,
        }

        # 保存日期快照
        dated_path = self._snapshot_path(date.fromisoformat(snapshot.as_of_date))
        dated_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n")

        # 保存最新快照
        latest_path = self.cache_dir / "latest.json"
        latest_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n")

        logger.debug(f"Saved snapshot to {dated_path}")


def resolve_nasdaq_100_symbols(
    cache_dir: Optional[Path] = None,
    as_of_date: Optional[date] = None,
) -> List[str]:
    """
    获取 NASDAQ-100 成分股的便捷函数

    Args:
        cache_dir: 缓存目录
        as_of_date: 目标日期

    Returns:
        股票代码列表
    """
    fetcher = ConstituentsFetcher(cache_dir=cache_dir)
    return fetcher.get_constituents(as_of_date=as_of_date)


if __name__ == "__main__":
    # 测试成分股获取
    print("Testing NASDAQ-100 Constituents Fetcher...")
    print("=" * 50)

    # 创建缓存目录
    cache_dir = Path(__file__).resolve().parent.parent / "data" / "cache" / "constituents"

    fetcher = ConstituentsFetcher(cache_dir=cache_dir)
    symbols = fetcher.get_constituents()

    print(f"\n✅ Fetched {len(symbols)} symbols:")
    print(f"   First 10: {symbols[:10]}")
    print(f"   Last 10: {symbols[-10:]}")
