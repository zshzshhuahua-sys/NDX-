"""
NASDAQ-100 200-Day Moving Average Breadth Indicator

指标定义:
Breadth200(t) = Count[ Close(i,t) > SMA200(i,t) ] / 有效股票数 × 100

用法:
    python src/ndx_breadth.py [日期] [--save] [--no-sector]
    python src/ndx_breadth.py --help
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from logging import getLogger
from pathlib import Path
from typing import List, Optional

import pandas as pd
import yfinance as yf

from constituents import HARDCODE_NASDAQ_100, resolve_nasdaq_100_symbols
from sectors import FinnhubSectorProvider, SectorBreadthService, SectorSQLiteStorage
from storage import JsonParquetRepository, StockInfo, InvalidStock


# 配置常量
MIN_HISTORY_DAYS: int = 200  # 最小200交易日
MIN_VALID_STOCKS: int = 80   # 最低有效股票数阈值
DEFAULT_LOOKBACK_DAYS: int = 500  # 默认回看天数
HISTORICAL_LOOKBACK_DAYS: int = 700  # 历史查询回看天数
TOP_STOCKS_LIMIT: int = 5  # 显示 Top N 股票

# 日志配置
logger = getLogger(__name__)


@dataclass
class BreadthResult:
    """宽度指标结果"""
    trade_date: str
    total_constituents: int
    valid_stocks: int
    above_200ma: int
    below_200ma: int
    invalid_stocks: int
    breadth_pct: float
    symbols_above: List[StockInfo] = field(default_factory=list)
    symbols_below: List[StockInfo] = field(default_factory=list)
    symbols_invalid: List[InvalidStock] = field(default_factory=list)

    def to_dict(self) -> dict:
        """转换为字典格式"""
        return {
            'trade_date': self.trade_date,
            'breadth_pct': self.breadth_pct,
            'valid_stocks': self.valid_stocks,
            'above_200ma': self.above_200ma,
            'below_200ma': self.below_200ma,
            'invalid_stocks': self.invalid_stocks,
        }


def _extract_close_matrix(data: pd.DataFrame) -> pd.DataFrame:
    """
    从yfinance数据中提取Close价格矩阵
    兼容不同yfinance版本的数据格式
    """
    # 情况1: MultiIndex (ticker, field)
    if isinstance(data.columns, pd.MultiIndex):
        if 'Close' in data.columns.get_level_values(1):
            result = data.xs('Close', axis=1, level=1)
        elif 'close' in data.columns.get_level_values(1):
            result = data.xs('close', axis=1, level=1)
        else:
            first_level = list(data.columns.get_level_values(0))
            if 'Close' in first_level:
                result = data['Close']
            elif 'close' in first_level:
                result = data['close']
            else:
                raise ValueError(f"无法解析MultiIndex数据: {data.columns[:5]}")

        if isinstance(result.columns, pd.MultiIndex):
            result.columns = [c[0] if isinstance(c, tuple) else c for c in result.columns]
        return result

    # 情况2: 普通DataFrame
    if 'Close' in data.columns:
        return data[['Close']]

    # 情况3: 单列
    if len(data.columns) == 1:
        return data

    raise ValueError(f"无法解析数据列结构: {data.columns[:5]}")


def download_stock_data(
    symbols: List[str],
    as_of_date: Optional[datetime] = None,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS
) -> pd.DataFrame:
    """下载股票历史数据"""
    end_date = as_of_date or datetime.now()
    start_date = end_date - timedelta(days=lookback_days)

    logger.info("下载 %d 只股票历史数据...", len(symbols))

    data = yf.download(
        tickers=symbols,
        start=start_date.strftime('%Y-%m-%d'),
        end=end_date.strftime('%Y-%m-%d'),
        interval="1d",
        auto_adjust=True,
        threads=True,
        group_by="ticker",
        progress=False
    )

    if data.empty:
        raise ValueError("数据下载失败: 返回空DataFrame")

    close_matrix = _extract_close_matrix(data)

    logger.info("下载完成: %d 个交易日, %d 只股票",
                close_matrix.shape[0], close_matrix.shape[1])

    if close_matrix.shape[0] < MIN_HISTORY_DAYS:
        raise ValueError(
            f"数据不足: 只有 {close_matrix.shape[0]} 个交易日，"
            f"需要至少 {MIN_HISTORY_DAYS} 个交易日"
        )

    return close_matrix


def calculate_sma200_vectorized(close_matrix: pd.DataFrame) -> pd.DataFrame:
    """向量化计算200日均线"""
    return close_matrix.rolling(
        window=MIN_HISTORY_DAYS,
        min_periods=MIN_HISTORY_DAYS
    ).mean()


def _parse_date(date_str: str) -> datetime:
    """解析并验证日期字符串"""
    try:
        return datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        raise ValueError(f"无效日期格式: {date_str}，请使用 YYYY-MM-DD 格式")


def calculate_breadth(
    close_matrix: pd.DataFrame,
    as_of_date: Optional[str] = None
) -> BreadthResult:
    """计算NASDAQ-100 200日均线宽度指标"""
    symbols: List[str] = list(close_matrix.columns)

    # 解析日期
    if as_of_date is not None:
        as_of_date_str = as_of_date
        _parse_date(as_of_date_str)
    else:
        as_of_date_str = close_matrix.index.max().strftime('%Y-%m-%d')

    # 向前找到最近的交易日
    target_date = None
    for date in reversed(close_matrix.index):
        if date.strftime('%Y-%m-%d') <= as_of_date_str:
            target_date = date
            break

    if target_date is None:
        raise ValueError(f"没有找到 {as_of_date_str} 之前的数据")

    logger.info("计算日期: %s", target_date.date())

    # 提取数据
    close_at_date = close_matrix.loc[target_date]
    sma200_matrix = calculate_sma200_vectorized(close_matrix)
    sma200_at_date = sma200_matrix.loc[target_date]

    # 判断突破
    above_mask = close_at_date > sma200_at_date
    valid_mask = ~sma200_at_date.isna() & ~close_at_date.isna()

    above_count = int(above_mask[valid_mask].sum())
    valid_count = int(valid_mask.sum())
    below_count = valid_count - above_count

    # 无效股票
    symbols_invalid: List[InvalidStock] = []
    for sym in symbols:
        if pd.isna(sma200_at_date[sym]):
            symbols_invalid.append(InvalidStock(sym, "数据不足200天"))
        elif pd.isna(close_at_date[sym]):
            symbols_invalid.append(InvalidStock(sym, "当日无数据"))

    # 突破/跌破详情
    symbols_above: List[StockInfo] = []
    symbols_below: List[StockInfo] = []

    for sym in symbols:
        if not valid_mask[sym]:
            continue

        close = float(close_at_date[sym])
        sma = float(sma200_at_date[sym])
        deviation = (close - sma) / sma * 100

        stock_info = StockInfo(
            symbol=sym,
            close=close,
            sma200=sma,
            deviation=deviation
        )

        if above_mask[sym]:
            symbols_above.append(stock_info)
        else:
            symbols_below.append(stock_info)

    if valid_count == 0:
        raise ValueError("没有有效股票，无法计算宽度指标")

    breadth_pct = above_count / valid_count * 100

    if valid_count < MIN_VALID_STOCKS:
        logger.warning("有效股票数(%d)低于阈值(%d)", valid_count, MIN_VALID_STOCKS)

    return BreadthResult(
        trade_date=target_date.strftime('%Y-%m-%d'),
        total_constituents=len(symbols),
        valid_stocks=valid_count,
        above_200ma=above_count,
        below_200ma=below_count,
        invalid_stocks=len(symbols_invalid),
        breadth_pct=round(breadth_pct, 2),
        symbols_above=symbols_above,
        symbols_below=symbols_below,
        symbols_invalid=symbols_invalid
    )


def print_sector_report(result: BreadthResult) -> None:
    """打印行业宽度报告"""
    print("\n" + "=" * 60)
    print("行业宽度指标")
    print("=" * 60)

    try:
        # 创建行业服务
        data_dir = Path(__file__).parent.parent / "data"
        cache_dir = data_dir / "cache" / "sectors"
        sqlite_storage = SectorSQLiteStorage(
            db_path=data_dir / "sectors.db",
            ttl_days=7,
        )
        sqlite_storage.initialize_sync()
        provider = FinnhubSectorProvider(
            cache_dir=cache_dir,
            sqlite_storage=sqlite_storage,
        )
        service = SectorBreadthService(provider)

        sector_results = service.calculate_sector_breadth(
            symbols_above=result.symbols_above,
            symbols_below=result.symbols_below,
        )

        sorted_sectors = sorted(
            sector_results.values(),
            key=lambda x: x.breadth_pct,
            reverse=True
        )

        for sr in sorted_sectors:
            bar = "#" * int(sr.breadth_pct / 5)
            print(f"{sr.sector_code:10} {sr.breadth_pct:5.1f}% {bar} ({sr.above_200ma}/{sr.total_stocks})")

    except Exception as exc:
        logger.warning("行业报告生成失败: %s", exc)
        print("行业报告暂时不可用")

    print("=" * 60)


def print_report(result: BreadthResult) -> None:
    """打印结果报告"""
    print("\n" + "=" * 60)
    print("NASDAQ-100 200日均线宽度指标")
    print("=" * 60)
    print(f"交易日: {result.trade_date}")
    print(f"指标数值: {result.breadth_pct}%")
    print("-" * 60)
    print(f"在均线上: {result.above_200ma} 只")
    print(f"在均线下: {result.below_200ma} 只")
    print(f"无效股票: {result.invalid_stocks} 只")
    print(f"有效总数: {result.valid_stocks} / {result.total_constituents}")
    print("-" * 60)

    if result.symbols_above:
        print(f"\n突破均线 (Top {TOP_STOCKS_LIMIT}):")
        sorted_above = sorted(result.symbols_above, key=lambda x: x.deviation, reverse=True)[:TOP_STOCKS_LIMIT]
        for s in sorted_above:
            print(f"   {s.symbol:6} {s.close:8.2f} / SMA200={s.sma200:8.2f} (+{s.deviation:.1f}%)")

    if result.symbols_below:
        print(f"\n跌破均线 (Top {TOP_STOCKS_LIMIT}):")
        sorted_below = sorted(result.symbols_below, key=lambda x: x.deviation)[:TOP_STOCKS_LIMIT]
        for s in sorted_below:
            print(f"   {s.symbol:6} {s.close:8.2f} / SMA200={s.sma200:8.2f} ({s.deviation:.1f}%)")

    print("=" * 60)


def main(
    as_of_date: Optional[str] = None,
    save: bool = False,
    show_sector: bool = True,
    data_dir: Optional[Path] = None,
) -> BreadthResult:
    """主函数"""
    print("NASDAQ-100 200日均线宽度指标计算")
    print("=" * 60)

    # 动态获取成分股
    print("获取NASDAQ-100成分股列表...")
    constituents_source = "unknown"
    try:
        symbols = resolve_nasdaq_100_symbols()
        print(f"   成功获取 {len(symbols)} 只成分股")
        constituents_source = "nasdaq_api"
    except Exception as e:
        logger.warning("动态获取失败: %s", e)
        symbols = HARDCODE_NASDAQ_100
        print(f"   使用备用列表 {len(symbols)} 只")
        constituents_source = "fallback"

    # 解析日期
    if as_of_date is not None:
        parsed_date = _parse_date(as_of_date)
        end_date = parsed_date + timedelta(days=1)
    else:
        end_date = None

    # 下载数据
    lookback_days = DEFAULT_LOOKBACK_DAYS if as_of_date is None else HISTORICAL_LOOKBACK_DAYS
    close_matrix = download_stock_data(
        symbols,
        as_of_date=end_date,
        lookback_days=lookback_days
    )

    # 计算宽度指标
    result = calculate_breadth(close_matrix, as_of_date)

    # 打印报告
    print_report(result)

    # 行业报告
    if show_sector:
        print_sector_report(result)

    # 保存结果
    if save:
        if data_dir is None:
            data_dir = Path(__file__).parent.parent / "data"
        repo = JsonParquetRepository(data_dir)
        repo.save(
            trade_date=result.trade_date,
            breadth_pct=result.breadth_pct,
            valid_stocks=result.valid_stocks,
            above_200ma=result.above_200ma,
            below_200ma=result.below_200ma,
            invalid_stocks=result.invalid_stocks,
            symbols_above=[{"symbol": s.symbol, "close": s.close, "sma200": s.sma200, "deviation": s.deviation} for s in result.symbols_above],
            symbols_below=[{"symbol": s.symbol, "close": s.close, "sma200": s.sma200, "deviation": s.deviation} for s in result.symbols_below],
            symbols_invalid=[{"symbol": s.symbol, "reason": s.reason} for s in result.symbols_invalid],
            constituents_source=constituents_source,
            data_source="yfinance",
        )
        print(f"   已保存到 {data_dir}")

    return result


def parse_args():
    """解析命令行参数"""
    import argparse

    parser = argparse.ArgumentParser(
        description="NASDAQ-100 200日均线宽度指标计算",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
    python ndx_breadth.py                      # 最新数据
    python ndx_breadth.py 2026-03-15         # 指定日期
    python ndx_breadth.py --save              # 保存结果
    python ndx_breadth.py --no-sector         # 跳过行业报告
        """
    )

    parser.add_argument(
        'date',
        nargs='?',
        default=None,
        help='计算日期 (YYYY-MM-DD格式)'
    )
    parser.add_argument(
        '--save', '-s',
        action='store_true',
        help='保存结果到数据目录'
    )
    parser.add_argument(
        '--no-sector',
        action='store_true',
        help='跳过行业宽度报告'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='显示详细日志'
    )

    return parser.parse_args()


if __name__ == "__main__":
    import sys

    args = parse_args()

    # 配置日志
    import logging
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        stream=sys.stdout
    )

    try:
        result = main(
            as_of_date=args.date,
            save=args.save,
            show_sector=not args.no_sector,
        )
        logger.info("计算完成")
    except Exception as exc:
        logger.error("执行失败: %s", exc)
        sys.exit(1)
