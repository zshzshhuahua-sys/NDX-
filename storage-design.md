# NDX 200日均线宽度指标 - 数据存储设计

## 1. 设计目标

| 目标 | 描述 |
|------|------|
| 时序存储 | 每日计算结果持久化，支持历史回溯 |
| 高效查询 | 按日期范围查询、聚合分析 |
| 轻量级 | 避免重型数据库，优先本地文件 |
| 可扩展 | 预留 PostgreSQL 迁移路径 |

## 2. 数据分层存储方案

### 2.1 存储层次

```
data/
├── breadth/                    # 宽度指标主数据
│   ├── daily/                  # 每日快照 (JSON)
│   │   └── 2026/
│   │       └── 04/
│   │           └── 2026-04-04.json
│   └── history/                # 历史批次数据 (Parquet)
│       └── breadth_history.parquet
├── raw/                        # 原始下载数据缓存
│   └── {symbol}/price_history.parquet
├── constituents/               # 成分股快照
│   ├── latest.json
│   └── 2026-04-04.json
└── meta/                       # 元数据
    ├── schema_version.json     # 当前schema版本
    └── last_update.json        # 最后更新时间
```

### 2.2 存储格式选择

| 格式 | 适用场景 | 理由 |
|------|----------|------|
| **Parquet** | 大批量历史分析 | 列式存储，压缩率高，支持快速聚合 |
| **JSON** | 单日快照、API响应 | 人类可读，易于调试，层级清晰 |
| **SQLite** | 中等规模时序数据 | 轻量，SQL查询能力，可迁移至PostgreSQL |

**推荐策略**: JSON (日快照) + Parquet (历史归档) + SQLite (查询引擎)

## 3. 数据模型

### 3.1 BreadthResult 日快照 (JSON)

```json
{
  "schema_version": "1.0",
  "trade_date": "2026-04-04",
  "breadth_pct": 72.0,
  "total_constituents": 104,
  "valid_stocks": 98,
  "above_200ma": 72,
  "below_200ma": 26,
  "invalid_stocks": 6,
  "calculated_at": "2026-04-04T18:30:00Z",
  "data_source": "yfinance",
  "constituents_source": "nasdaq_api",
  "symbols_above": [
    {
      "symbol": "NVDA",
      "close": 110.23,
      "sma200": 85.50,
      "deviation": 28.93
    }
  ],
  "symbols_below": [
    {
      "symbol": "AAPL",
      "close": 170.50,
      "sma200": 175.20,
      "deviation": -2.68
    }
  ],
  "symbols_invalid": [
    {
      "symbol": "DELISTED",
      "reason": "数据不足200天"
    }
  ]
}
```

### 3.2 Parquet 历史表结构

```python
# Schema: breadth_history.parquet

breadth_pct: float64           # 宽度百分比 (0-100)
trade_date: date               # 交易日
valid_stocks: int32            # 有效股票数
above_200ma: int32             # 突破均线数
below_200ma: int32             # 跌破均线数
invalid_stocks: int32          # 无效股票数
calculated_at: timestamp       # 计算时间
```

### 3.3 SQLite 表结构 (可选查询层)

```sql
CREATE TABLE breadth_history (
    id SERIAL PRIMARY KEY,
    trade_date DATE UNIQUE NOT NULL,
    breadth_pct DECIMAL(5,2) NOT NULL,
    total_constituents INT NOT NULL,
    valid_stocks INT NOT NULL,
    above_200ma INT NOT NULL,
    below_200ma INT NOT NULL,
    invalid_stocks INT NOT NULL,
    calculated_at TIMESTAMP NOT NULL,
    data_source TEXT,
    constituents_source TEXT
);

CREATE INDEX idx_trade_date ON breadth_history(trade_date);
CREATE INDEX idx_breadth_pct ON breadth_history(breadth_pct);

-- 股票详细表
CREATE TABLE stock_details (
    id SERIAL PRIMARY KEY,
    trade_date DATE NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    close DECIMAL(10,4),
    sma200 DECIMAL(10,4),
    deviation DECIMAL(8,4),
    status VARCHAR(10) NOT NULL,  -- 'above', 'below', 'invalid'
    invalid_reason TEXT,
    FOREIGN KEY (trade_date) REFERENCES breadth_history(trade_date)
);

CREATE INDEX idx_stock_trade_date ON stock_details(trade_date);
CREATE INDEX idx_stock_symbol ON stock_details(symbol);
```

## 4. API 接口设计

### 4.1 StorageService 接口

```python
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date
from typing import List, Optional
from pathlib import Path

@dataclass(frozen=True)
class BreadthRecord:
    """单条宽度记录"""
    trade_date: date
    breadth_pct: float
    valid_stocks: int
    above_200ma: int
    below_200ma: int
    invalid_stocks: int

@dataclass(frozen=True)
class BreadthDetail(BreadthRecord):
    """带详情的宽度记录"""
    symbols_above: List[StockInfo]
    symbols_below: List[StockInfo]
    symbols_invalid: List[InvalidStock]


class BreadthRepository(ABC):
    """宽度数据仓库接口"""

    @abstractmethod
    def save(self, result: BreadthResult) -> None:
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
        end_date: date
    ) -> List[BreadthRecord]:
        """查询日期范围内的数据"""
        pass

    @abstractmethod
    def find_latest(self) -> Optional[BreadthDetail]:
        """获取最新数据"""
        pass
```

### 4.2 实现类

```python
class JsonParquetRepository(BreadthRepository):
    """
    JSON + Parquet 混合存储实现

    - 日常写入: JSON 快照
    - 历史归档: Parquet 批量
    - 查询: 按需从 JSON/Parquet 读取
    """

    def __init__(
        self,
        data_dir: Path,
        json_dir: Optional[Path] = None,
        parquet_path: Optional[Path] = None
    ):
        self.data_dir = data_dir
        self.json_dir = json_dir or data_dir / "breadth" / "daily"
        self.parquet_path = parquet_path or data_dir / "breadth" / "history" / "breadth_history.parquet"

    def save(self, result: BreadthResult) -> None:
        """保存到 JSON + 追加到 Parquet"""
        self._save_json(result)
        self._append_parquet(result)

    def _save_json(self, result: BreadthResult) -> None:
        """保存 JSON 快照"""
        date_path = Path(result.trade_date)
        year = date_path.year
        month = f"{date_path.month:02d}"

        dir_path = self.json_dir / str(year) / month
        dir_path.mkdir(parents=True, exist_ok=True)

        file_path = dir_path / f"{result.trade_date}.json"

        payload = {
            "schema_version": "1.0",
            **result.to_dict(),
            "calculated_at": datetime.utcnow().isoformat() + "Z",
            "symbols_above": [
                {"symbol": s.symbol, "close": s.close, "sma200": s.sma200, "deviation": s.deviation}
                for s in result.symbols_above
            ],
            "symbols_below": [
                {"symbol": s.symbol, "close": s.close, "sma200": s.sma200, "deviation": s.deviation}
                for s in result.symbols_below
            ],
            "symbols_invalid": [
                {"symbol": s.symbol, "reason": s.reason}
                for s in result.symbols_invalid
            ]
        }

        file_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n")

    def _append_parquet(self, result: BreadthResult) -> None:
        """追加到 Parquet 文件"""
        record = {
            "trade_date": pd.to_datetime(result.trade_date).date(),
            "breadth_pct": result.breadth_pct,
            "valid_stocks": result.valid_stocks,
            "above_200ma": result.above_200ma,
            "below_200ma": result.below_200ma,
            "invalid_stocks": result.invalid_stocks,
            "calculated_at": datetime.utcnow()
        }

        df_new = pd.DataFrame([record])

        if self.parquet_path.exists():
            df_existing = pd.read_parquet(self.parquet_path)
            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
        else:
            df_combined = df_new

        df_combined.to_parquet(self.parquet_path, engine="pyarrow", compression="snappy")

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
        end_date: date
    ) -> List[BreadthRecord]:
        """从 Parquet 读取日期范围"""
        if not self.parquet_path.exists():
            return []

        df = pd.read_parquet(self.parquet_path)
        df = df[
            (df["trade_date"] >= start_date) &
            (df["trade_date"] <= end_date)
        ].sort_values("trade_date")

        return [
            BreadthRecord(
                trade_date=row["trade_date"],
                breadth_pct=row["breadth_pct"],
                valid_stocks=row["valid_stocks"],
                above_200ma=row["above_200ma"],
                below_200ma=row["below_200ma"],
                invalid_stocks=row["invalid_stocks"]
            )
            for row in df.itertuples(index=False)
        ]

    def find_latest(self) -> Optional[BreadthDetail]:
        """获取最新 JSON 快照"""
        if not self.json_dir.exists():
            return None

        # 找最新的 JSON 文件
        files = list(self.json_dir.rglob("*.json"))
        if not files:
            return None

        latest = max(files, key=lambda p: p.stat().st_mtime)
        return self._parse_json_detail(latest)
```

### 4.3 PostgreSQL 迁移路径 (未来扩展)

```python
class PostgresRepository(BreadthRepository):
    """
    PostgreSQL 实现 - 未来迁移目标

    迁移步骤:
    1. 保留 JsonParquetRepository 作为缓存层
    2. 新增 PostgresRepository 处理所有写入
    3. 定期从 Parquet 导入历史数据
    4. 双写期间保持数据一致
    """

    def __init__(self, connection_string: str):
        import psycopg2
        self.conn = psycopg2.connect(connection_string)

    def save(self, result: BreadthResult) -> None:
        """保存到 PostgreSQL"""
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO breadth_history (
                    trade_date, breadth_pct, total_constituents,
                    valid_stocks, above_200ma, below_200ma,
                    invalid_stocks, calculated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (trade_date) DO UPDATE SET
                    breadth_pct = EXCLUDED.breadth_pct,
                    valid_stocks = EXCLUDED.valid_stocks,
                    above_200ma = EXCLUDED.above_200ma,
                    below_200ma = EXCLUDED.below_200ma,
                    invalid_stocks = EXCLUDED.invalid_stocks,
                    calculated_at = EXCLUDED.calculated_at
            """, (
                result.trade_date, result.breadth_pct, result.total_constituents,
                result.valid_stocks, result.above_200ma, result.below_200ma,
                result.invalid_stocks, datetime.utcnow()
            ))

            # 保存详细数据
            for s in result.symbols_above:
                cur.execute("""
                    INSERT INTO stock_details (
                        trade_date, symbol, close, sma200, deviation, status
                    ) VALUES (%s, %s, %s, %s, %s, 'above')
                    ON CONFLICT DO NOTHING
                """, (result.trade_date, s.symbol, s.close, s.sma200, s.deviation))

            # ... symbols_below, symbols_invalid 类似
            self.conn.commit()
```

## 5. 数据保留策略

| 数据类型 | 保留期限 | 存储格式 | 理由 |
|----------|----------|----------|------|
| 每日快照 (JSON) | 永久 | JSON | 易于查阅、调试、审计 |
| 历史汇总 (Parquet) | 永久 | Parquet | 高效分析、压缩存储 |
| 股票详细 (Parquet) | 2年 | Parquet | 详情数据量大，按需查询 |
| 原始价格数据 | 90天 | Parquet | 避免重复下载 |
| 成分股快照 | 30天 | JSON | 日常缓存需求 |

### 5.1 归档策略

```python
# 每月执行: 将每日 JSON 归档为月度 Parquet
def archive_monthly(year: int, month: int) -> None:
    """归档月度数据到 Parquet"""
    json_dir = data_dir / "breadth" / "daily" / str(year) / f"{month:02d}"
    parquet_path = data_dir / "breadth" / "archive" / f"{year}_{month:02d}.parquet"

    files = sorted(json_dir.glob("*.json"))
    records = []

    for f in files:
        data = json.loads(f.read_text())
        records.append({
            "trade_date": data["trade_date"],
            "breadth_pct": data["breadth_pct"],
            "valid_stocks": data["valid_stocks"],
            "above_200ma": data["above_200ma"],
            "below_200ma": data["below_200ma"],
            "invalid_stocks": data["invalid_stocks"]
        })

    if records:
        df = pd.DataFrame(records)
        parquet_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(parquet_path, engine="pyarrow", compression="snappy")

    # 归档后可删除原始 JSON (可选)
    # for f in files:
    #     f.unlink()
```

### 5.2 清理策略

```bash
# 保留规则
find data/raw -name "*.parquet" -mtime +90 -delete  # 90天原始数据
find data/constituents -name "*.json" -mtime +30 -delete  # 30天成分股快照
```

## 6. 目录结构

```
ndx-200ma-breadth/
├── src/
│   ├── ndx_breadth.py          # 核心计算
│   ├── constituents.py        # 成分股获取
│   └── storage/               # 新增: 存储模块
│       ├── __init__.py
│       ├── repository.py       # 仓库接口
│       ├── json_parquet_repo.py # JSON+Parquet实现
│       ├── sqlite_repo.py      # SQLite实现(可选)
│       └── postgres_repo.py    # PostgreSQL实现(未来)
├── data/                       # 数据目录
│   ├── breadth/
│   │   ├── daily/             # 每日JSON快照
│   │   │   └── 2026/
│   │   │       └── 04/
│   │   │           └── 2026-04-04.json
│   │   ├── history/
│   │   │   └── breadth_history.parquet
│   │   └── archive/           # 归档月度数据
│   │       └── 2026_04.parquet
│   ├── raw/                   # 原始下载缓存
│   ├── constituents/          # 成分股快照
│   └── meta/                  # 元数据
├── tests/
│   ├── test_breadth.py
│   └── test_storage.py        # 新增: 存储测试
├── storage-design.md          # 本文档
└── requirements.txt
```

## 7. 使用示例

```python
from pathlib import Path
from ndx_breadth import main as calculate_breadth
from storage import JsonParquetRepository, PostgresRepository

# 初始化仓库
data_dir = Path(__file__).parent / "data"
repo = JsonParquetRepository(data_dir)

# 计算并保存
result = calculate_breadth()  # 现有逻辑
repo.save(result)

# 查询最新
latest = repo.find_latest()
print(f"最新宽度: {latest.breadth_pct}%")

# 查询历史
records = repo.find_range(
    start_date=date(2026, 1, 1),
    end_date=date(2026, 3, 31)
)
for r in records:
    print(f"{r.trade_date}: {r.breadth_pct}%")

# 迁移到 PostgreSQL (未来)
# repo = PostgresRepository(connection_string="postgresql://...")
# repo.save(result)
```

## 8. Schema 版本管理

```json
// data/meta/schema_version.json
{
  "current_version": "1.0",
  "migrations": [
    {
      "version": "1.0",
      "applied_at": "2026-04-04T00:00:00Z",
      "description": "Initial schema"
    }
  ]
}
```

## 9. 实施优先级

| 阶段 | 任务 | 工作量 |
|------|------|--------|
| P0 | JSON 日快照存储 | 1天 |
| P0 | Parquet 历史存储 | 1天 |
| P1 | SQLite 查询层 (可选) | 2天 |
| P2 | 历史数据导入脚本 | 1天 |
| P2 | 定时归档任务 | 1天 |
| P3 | PostgreSQL 迁移 | 3天 |

## 10. 依赖更新

```
# requirements.txt
yfinance>=0.2.36
pandas>=2.0.0
pyarrow>=14.0.0
# 新增
psycopg2-binary>=2.9.9  # PostgreSQL (未来)
```

---

*文档版本: 1.0*
*创建日期: 2026-04-04*
