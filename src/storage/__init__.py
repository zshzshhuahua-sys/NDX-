"""
NDX 宽度指标存储模块

支持:
- JSON 日快照存储
- Parquet 历史归档
- Repository 接口设计
"""

from storage.repository import (
    BreadthRecord,
    BreadthDetail,
    StockInfo,
    InvalidStock,
    BreadthRepository,
)
from storage.json_parquet_repo import JsonParquetRepository

__all__ = [
    "BreadthRecord",
    "BreadthDetail",
    "StockInfo",
    "InvalidStock",
    "BreadthRepository",
    "JsonParquetRepository",
]
