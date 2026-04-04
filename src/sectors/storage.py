"""
行业分类 SQLite 存储层

提供本地 SQLite 数据库存储行业分类数据，支持 TTL 过期机制
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import aiosqlite

from .fallback import NASDAQ100_SECTOR_MAPPING
from .models import StockSector


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s"
)
logger = logging.getLogger(__name__)

# 默认 TTL: 7 天
DEFAULT_TTL_DAYS = 7


@dataclass(frozen=True)
class StockSectorRecord:
    """存储记录"""
    symbol: str
    sector: str
    sector_code: str
    industry: str
    source: str  # 'finnhub', 'fallback', 'manual'
    fetched_at: float  # Unix timestamp
    expires_at: float   # Unix timestamp

    def is_expired(self) -> bool:
        """检查是否过期"""
        return time.time() > self.expires_at

    def to_stock_sector(self) -> StockSector:
        """转换为 StockSector"""
        return StockSector(
            symbol=self.symbol,
            sector=self.sector,
            sector_code=self.sector_code,
            industry=self.industry,
        )


class SectorSQLiteStorage:
    """
    SQLite 行业分类存储

    特性:
    - 本地 SQLite 数据库持久化
    - TTL 过期机制
    - 支持批量操作
    - 异步接口
    """

    def __init__(
        self,
        db_path: Optional[Path] = None,
        ttl_days: int = DEFAULT_TTL_DAYS,
    ) -> None:
        self.db_path = db_path or self._default_db_path()
        self.ttl_days = ttl_days
        self._init_done = False

    def _default_db_path(self) -> Path:
        """获取默认数据库路径"""
        return Path.home() / ".ndx-breadth" / "sectors.db"

    def initialize_sync(self) -> None:
        """同步初始化数据库（用于同步上下文）"""
        if self._init_done:
            return

        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        # 使用同步方式初始化（aiosqlite 也支持同步调用）
        import sqlite3
        conn = sqlite3.connect(str(self.db_path))
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS stock_sectors (
                    symbol TEXT PRIMARY KEY,
                    sector TEXT NOT NULL,
                    sector_code TEXT NOT NULL,
                    industry TEXT NOT NULL,
                    source TEXT NOT NULL DEFAULT 'fallback',
                    fetched_at REAL NOT NULL,
                    expires_at REAL NOT NULL
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_expires_at
                ON stock_sectors(expires_at)
            """)
            conn.commit()
        finally:
            conn.close()

        self._init_done = True
        import logging
        logging.getLogger(__name__).info(f"Initialized sector storage at {self.db_path}")

    async def initialize(self) -> None:
        """初始化数据库"""
        if self._init_done:
            return

        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS stock_sectors (
                    symbol TEXT PRIMARY KEY,
                    sector TEXT NOT NULL,
                    sector_code TEXT NOT NULL,
                    industry TEXT NOT NULL,
                    source TEXT NOT NULL DEFAULT 'fallback',
                    fetched_at REAL NOT NULL,
                    expires_at REAL NOT NULL
                )
            """)
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_expires_at
                ON stock_sectors(expires_at)
            """)
            await db.commit()

        self._init_done = True
        logger.info(f"Initialized sector storage at {self.db_path}")

    async def close(self) -> None:
        """关闭数据库连接"""
        self._init_done = False

    async def get(self, symbol: str) -> Optional[StockSectorRecord]:
        """
        获取单只股票的行业分类

        Args:
            symbol: 股票代码

        Returns:
            StockSectorRecord 或 None（不存在或已过期）
        """
        await self.initialize()

        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                """
                SELECT symbol, sector, sector_code, industry, source,
                       fetched_at, expires_at
                FROM stock_sectors
                WHERE symbol = ?
                """,
                (symbol,)
            )
            row = await cursor.fetchone()

        if row is None:
            return None

        record = StockSectorRecord(
            symbol=row["symbol"],
            sector=row["sector"],
            sector_code=row["sector_code"],
            industry=row["industry"],
            source=row["source"],
            fetched_at=row["fetched_at"],
            expires_at=row["expires_at"],
        )

        # 检查是否过期
        if record.is_expired():
            await self.delete(symbol)
            return None

        return record

    async def get_batch(self, symbols: List[str]) -> Dict[str, Optional[StockSectorRecord]]:
        """
        批量获取行业分类

        Args:
            symbols: 股票代码列表

        Returns:
            symbol -> StockSectorRecord 的映射
        """
        await self.initialize()

        results: Dict[str, Optional[StockSectorRecord]] = {s: None for s in symbols}

        if not symbols:
            return results

        placeholders = ",".join(["?" for _ in symbols])
        current_time = time.time()

        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                f"""
                SELECT symbol, sector, sector_code, industry, source,
                       fetched_at, expires_at
                FROM stock_sectors
                WHERE symbol IN ({placeholders})
                """,
                symbols
            )
            rows = await cursor.fetchall()

        for row in rows:
            record = StockSectorRecord(
                symbol=row["symbol"],
                sector=row["sector"],
                sector_code=row["sector_code"],
                industry=row["industry"],
                source=row["source"],
                fetched_at=row["fetched_at"],
                expires_at=row["expires_at"],
            )

            # 检查是否过期
            if record.is_expired():
                results[row["symbol"]] = None
            else:
                results[row["symbol"]] = record

        return results

    async def save(self, record: StockSectorRecord) -> None:
        """
        保存行业分类记录

        Args:
            record: StockSectorRecord
        """
        await self.initialize()

        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT OR REPLACE INTO stock_sectors
                (symbol, sector, sector_code, industry, source, fetched_at, expires_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.symbol,
                    record.sector,
                    record.sector_code,
                    record.industry,
                    record.source,
                    record.fetched_at,
                    record.expires_at,
                )
            )
            await db.commit()

    async def save_batch(self, records: List[StockSectorRecord]) -> None:
        """
        批量保存行业分类记录

        Args:
            records: StockSectorRecord 列表
        """
        if not records:
            return

        await self.initialize()

        async with aiosqlite.connect(self.db_path) as db:
            await db.executemany(
                """
                INSERT OR REPLACE INTO stock_sectors
                (symbol, sector, sector_code, industry, source, fetched_at, expires_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        r.symbol,
                        r.sector,
                        r.sector_code,
                        r.industry,
                        r.source,
                        r.fetched_at,
                        r.expires_at,
                    )
                    for r in records
                ]
            )
            await db.commit()

    async def delete(self, symbol: str) -> None:
        """删除记录"""
        await self.initialize()

        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "DELETE FROM stock_sectors WHERE symbol = ?",
                (symbol,)
            )
            await db.commit()

    async def delete_expired(self) -> int:
        """
        删除所有过期记录

        Returns:
            删除的记录数
        """
        await self.initialize()
        current_time = time.time()

        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                "DELETE FROM stock_sectors WHERE expires_at < ?",
                (current_time,)
            )
            await db.commit()
            deleted = cursor.rowcount

        if deleted > 0:
            logger.info(f"Deleted {deleted} expired sector records")

        return deleted

    async def count(self) -> int:
        """获取记录总数"""
        await self.initialize()

        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                "SELECT COUNT(*) FROM stock_sectors"
            )
            row = await cursor.fetchone()

        return row[0] if row else 0

    async def get_stats(self) -> Dict[str, int]:
        """获取存储统计"""
        await self.initialize()
        current_time = time.time()

        async with aiosqlite.connect(self.db_path) as db:
            # 总数
            cursor = await db.execute("SELECT COUNT(*) FROM stock_sectors")
            total = (await cursor.fetchone())[0]

            # 有效数
            cursor = await db.execute(
                "SELECT COUNT(*) FROM stock_sectors WHERE expires_at >= ?",
                (current_time,)
            )
            valid = (await cursor.fetchone())[0]

            # 过期数
            cursor = await db.execute(
                "SELECT COUNT(*) FROM stock_sectors WHERE expires_at < ?",
                (current_time,)
            )
            expired = (await cursor.fetchone())[0]

            # 按来源统计
            cursor = await db.execute(
                """
                SELECT source, COUNT(*) as cnt
                FROM stock_sectors
                WHERE expires_at >= ?
                GROUP BY source
                """,
                (current_time,)
            )
            by_source = {row[0]: row[1] for row in await cursor.fetchall()}

        return {
            "total": total,
            "valid": valid,
            "expired": expired,
            "by_source": by_source,
        }

    async def import_fallback_data(self) -> int:
        """
        从 fallback.py 导入所有数据到 SQLite

        Returns:
            导入的记录数
        """
        await self.initialize()

        current_time = time.time()
        expires_at = current_time + (self.ttl_days * 24 * 3600 * 30)  # 30x TTL = 永久缓存

        records = [
            StockSectorRecord(
                symbol=symbol,
                sector=data["sector"],
                sector_code=data["sector_code"],
                industry=data["industry"],
                source="fallback",
                fetched_at=current_time,
                expires_at=expires_at,
            )
            for symbol, data in NASDAQ100_SECTOR_MAPPING.items()
        ]

        await self.save_batch(records)
        logger.info(f"Imported {len(records)} fallback sector records")

        return len(records)


def create_sector_record(
    symbol: str,
    sector: str,
    sector_code: str,
    industry: str,
    source: str = "fallback",
    ttl_days: int = DEFAULT_TTL_DAYS,
) -> StockSectorRecord:
    """创建行业分类记录"""
    current_time = time.time()
    return StockSectorRecord(
        symbol=symbol,
        sector=sector,
        sector_code=sector_code,
        industry=industry,
        source=source,
        fetched_at=current_time,
        expires_at=current_time + (ttl_days * 24 * 3600),
    )
