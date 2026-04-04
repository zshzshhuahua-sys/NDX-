"""
JSON + Parquet 混合存储实现

- 日常写入: JSON 快照
- 历史归档: Parquet 批量
- 查询: 按需从 JSON/Parquet 读取

修复问题 (CODEX Review):
- P0: 并发写入 - 添加文件锁
- P0: 日期类型不一致 - 统一使用 date 类型
- P1: find_latest() - 基于文件名解析日期
- P1: 缺少 schema 验证 - 添加验证
- P1: 缺少输入验证 - 验证 dict 结构
"""

from __future__ import annotations

import fcntl
import struct
from datetime import date, datetime
from pathlib import Path
from typing import List, Optional

import json
import pandas as pd

from storage.repository import (
    BreadthRecord,
    BreadthDetail,
    StockInfo,
    InvalidStock,
    BreadthRepository,
)


# Schema 验证字段
REQUIRED_STOCK_FIELDS = {"symbol", "close", "sma200", "deviation"}
REQUIRED_INVALID_FIELDS = {"symbol", "reason"}


class JsonParquetRepository(BreadthRepository):
    """
    JSON + Parquet 混合存储实现

    存储结构:
    - JSON: data/breadth/daily/YYYY/MM/YYYY-MM-DD.json
    - Parquet: data/breadth/history/breadth_history.parquet
    """

    SCHEMA_VERSION = "1.0"

    def __init__(
        self,
        data_dir: Path | str,
    ):
        self.data_dir = Path(data_dir)
        self.json_dir = self.data_dir / "breadth" / "daily"
        self.parquet_dir = self.data_dir / "breadth" / "history"
        self.parquet_path = self.parquet_dir / "breadth_history.parquet"
        self.lock_path = self.parquet_dir / ".breadth_history.lock"

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
        constituents_source: str = "nasdaq_api",
        data_source: str = "yfinance",
    ) -> None:
        """保存到 JSON + 追加到 Parquet（线程安全）"""
        # 验证输入结构
        self._validate_symbols(symbols_above, REQUIRED_STOCK_FIELDS, "symbols_above")
        self._validate_symbols(symbols_below, REQUIRED_STOCK_FIELDS, "symbols_below")
        self._validate_symbols(symbols_invalid, REQUIRED_INVALID_FIELDS, "symbols_invalid")

        self._save_json(
            trade_date=trade_date,
            breadth_pct=breadth_pct,
            valid_stocks=valid_stocks,
            above_200ma=above_200ma,
            below_200ma=below_200ma,
            invalid_stocks=invalid_stocks,
            symbols_above=symbols_above,
            symbols_below=symbols_below,
            symbols_invalid=symbols_invalid,
            constituents_source=constituents_source,
            data_source=data_source,
        )
        self._append_parquet(
            trade_date=trade_date,
            breadth_pct=breadth_pct,
            valid_stocks=valid_stocks,
            above_200ma=above_200ma,
            below_200ma=below_200ma,
            invalid_stocks=invalid_stocks,
        )

    def _validate_symbols(
        self,
        symbols: List[dict],
        required_fields: set,
        field_name: str,
    ) -> None:
        """验证 symbols 列表中每个 dict 的字段"""
        for i, item in enumerate(symbols):
            if not isinstance(item, dict):
                raise ValueError(
                    f"{field_name}[{i}] must be a dict, got {type(item).__name__}"
                )
            missing = required_fields - set(item.keys())
            if missing:
                raise ValueError(
                    f"{field_name}[{i}] missing fields: {missing}"
                )

    def _save_json(
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
        constituents_source: str,
        data_source: str,
    ) -> None:
        """保存 JSON 快照（线程安全，使用原子写入）"""
        dt = datetime.strptime(trade_date, "%Y-%m-%d")
        year = dt.year
        month = f"{dt.month:02d}"

        dir_path = self.json_dir / str(year) / month
        dir_path.mkdir(parents=True, exist_ok=True)

        file_path = dir_path / f"{trade_date}.json"

        payload = {
            "schema_version": self.SCHEMA_VERSION,
            "trade_date": trade_date,
            "breadth_pct": breadth_pct,
            "valid_stocks": valid_stocks,
            "above_200ma": above_200ma,
            "below_200ma": below_200ma,
            "invalid_stocks": invalid_stocks,
            "constituents_source": constituents_source,
            "data_source": data_source,
            "calculated_at": datetime.utcnow().isoformat() + "Z",
            "symbols_above": symbols_above,
            "symbols_below": symbols_below,
            "symbols_invalid": symbols_invalid,
        }

        # 原子写入：先写临时文件，再 rename
        temp_path = file_path.with_suffix(".tmp")
        try:
            temp_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n")
            temp_path.rename(file_path)  # 原子操作
        except IOError as e:
            raise IOError(f"Failed to write JSON file {file_path}: {e}") from e
        finally:
            if temp_path.exists():
                temp_path.unlink(missing_ok=True)

    def _append_parquet(
        self,
        trade_date: str,
        breadth_pct: float,
        valid_stocks: int,
        above_200ma: int,
        below_200ma: int,
        invalid_stocks: int,
    ) -> None:
        """追加到 Parquet 文件（线程安全，使用文件锁）"""
        self.parquet_dir.mkdir(parents=True, exist_ok=True)

        # 解析日期为 date 对象
        trade_date_obj = datetime.strptime(trade_date, "%Y-%m-%d").date()

        record = {
            "trade_date": trade_date_obj,
            "breadth_pct": breadth_pct,
            "valid_stocks": valid_stocks,
            "above_200ma": above_200ma,
            "below_200ma": below_200ma,
            "invalid_stocks": invalid_stocks,
            "calculated_at": datetime.utcnow(),
        }

        df_new = pd.DataFrame([record])

        # 使用文件锁保证并发安全
        with open(self.lock_path, "w") as lock_file:
            try:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)

                if self.parquet_path.exists():
                    df_existing = pd.read_parquet(str(self.parquet_path))
                    # 使用 date 类型比较（统一类型）
                    df_existing = df_existing[df_existing["trade_date"] != trade_date_obj]
                    df_combined = pd.concat([df_existing, df_new], ignore_index=True)
                else:
                    df_combined = df_new

                df_combined.to_parquet(
                    str(self.parquet_path),
                    engine="pyarrow",
                    compression="snappy",
                )
            finally:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)

    def find_by_date(self, trade_date: date) -> Optional[BreadthDetail]:
        """从 JSON 读取指定日期"""
        date_str = trade_date.strftime("%Y-%m-%d")
        year = trade_date.year
        month = f"{trade_date.month:02d}"

        file_path = self.json_dir / str(year) / month / f"{date_str}.json"

        if not file_path.exists():
            return None

        return self._parse_json_detail(file_path)

    def find_range(
        self,
        start_date: date,
        end_date: date,
    ) -> List[BreadthRecord]:
        """从 Parquet 读取日期范围"""
        if not self.parquet_path.exists():
            return []

        df = pd.read_parquet(str(self.parquet_path))
        df = df[
            (df["trade_date"] >= start_date) &
            (df["trade_date"] <= end_date)
        ].sort_values("trade_date")

        return [
            BreadthRecord(
                trade_date=row["trade_date"].date() if isinstance(row["trade_date"], pd.Timestamp) else row["trade_date"],
                breadth_pct=row["breadth_pct"],
                valid_stocks=row["valid_stocks"],
                above_200ma=row["above_200ma"],
                below_200ma=row["below_200ma"],
                invalid_stocks=row["invalid_stocks"],
                calculated_at=row.get("calculated_at"),
            )
            for _, row in df.iterrows()
        ]

    def find_latest(self) -> Optional[BreadthDetail]:
        """获取最新 JSON 快照（基于文件名日期）"""
        if not self.json_dir.exists():
            return None

        # 收集所有 JSON 文件并解析文件名中的日期
        files_with_dates: List[tuple] = []
        for file_path in self.json_dir.rglob("*.json"):
            try:
                # 文件名格式: YYYY-MM-DD.json
                date_str = file_path.stem  # 去掉 .json 后缀
                parsed_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                files_with_dates.append((parsed_date, file_path))
            except ValueError:
                # 跳过无法解析的文件名
                continue

        if not files_with_dates:
            return None

        # 按日期排序，取最新的
        files_with_dates.sort(key=lambda x: x[0], reverse=True)
        latest_date, latest_path = files_with_dates[0]

        return self._parse_json_detail(latest_path)

    def _parse_json_detail(self, file_path: Path) -> BreadthDetail:
        """解析 JSON 文件为 BreadthDetail（含 schema 验证）"""
        try:
            data = json.loads(file_path.read_text())
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in {file_path}: {e}") from e

        # Schema 验证
        self._validate_schema(data)

        symbols_above = [
            StockInfo(
                symbol=s["symbol"],
                close=float(s["close"]),
                sma200=float(s["sma200"]),
                deviation=float(s["deviation"]),
            )
            for s in data.get("symbols_above", [])
        ]

        symbols_below = [
            StockInfo(
                symbol=s["symbol"],
                close=float(s["close"]),
                sma200=float(s["sma200"]),
                deviation=float(s["deviation"]),
            )
            for s in data.get("symbols_below", [])
        ]

        symbols_invalid = [
            InvalidStock(
                symbol=s["symbol"],
                reason=s["reason"],
            )
            for s in data.get("symbols_invalid", [])
        ]

        trade_date = datetime.strptime(data["trade_date"], "%Y-%m-%d").date()
        calculated_at = None
        if data.get("calculated_at"):
            try:
                calc_at_str = data["calculated_at"].replace("Z", "+00:00")
                calculated_at = datetime.fromisoformat(calc_at_str)
            except ValueError:
                pass

        return BreadthDetail(
            trade_date=trade_date,
            breadth_pct=float(data["breadth_pct"]),
            valid_stocks=int(data["valid_stocks"]),
            above_200ma=int(data["above_200ma"]),
            below_200ma=int(data["below_200ma"]),
            invalid_stocks=int(data["invalid_stocks"]),
            symbols_above=symbols_above,
            symbols_below=symbols_below,
            symbols_invalid=symbols_invalid,
            constituents_source=data.get("constituents_source"),
            data_source=data.get("data_source"),
            calculated_at=calculated_at,
        )

    def _validate_schema(self, data: dict) -> None:
        """验证 JSON schema"""
        required_fields = {
            "schema_version",
            "trade_date",
            "breadth_pct",
            "valid_stocks",
            "above_200ma",
            "below_200ma",
            "invalid_stocks",
        }

        missing = required_fields - set(data.keys())
        if missing:
            raise ValueError(f"JSON missing required fields: {missing}")

        # 验证类型
        if not isinstance(data["trade_date"], str):
            raise ValueError("trade_date must be string")
        if not isinstance(data["breadth_pct"], (int, float)):
            raise ValueError("breadth_pct must be number")
        if not isinstance(data["valid_stocks"], int):
            raise ValueError("valid_stocks must be int")

        # 验证 symbols 字段类型
        for field_name in ["symbols_above", "symbols_below", "symbols_invalid"]:
            if field_name in data and not isinstance(data[field_name], list):
                raise ValueError(f"{field_name} must be a list")
