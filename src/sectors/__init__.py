"""
股票行业分类模块

提供 NASDAQ-100 成分股的行业分类功能
"""

from .models import (
    NASDAQ100_SECTORS,
    StockSector,
    SectorBreadthResult,
)
from .provider import FinnhubSectorProvider
from .service import SectorBreadthService
from .storage import (
    SectorSQLiteStorage,
    StockSectorRecord,
    create_sector_record,
)

__all__ = [
    "NASDAQ100_SECTORS",
    "StockSector",
    "SectorBreadthResult",
    "FinnhubSectorProvider",
    "SectorBreadthService",
    "SectorSQLiteStorage",
    "StockSectorRecord",
    "create_sector_record",
]
