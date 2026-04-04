"""
NASDAQ-100 200-Day Moving Average Breadth Indicator
原型版本 - 使用 yfinance

指标定义:
Breadth200(t) = Count[ Close(i,t) > SMA200(i,t) ] / 有效股票数 × 100

改进点 (CODEX Review):
- 修复日期对齐问题
- 向量化计算替代循环
- 增大下载天数确保200交易日
- 添加数据下载验证
- 使用数据实际最后日期
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional
import pandas as pd
import yfinance as yf

from constituents import resolve_nasdaq_100_symbols


MIN_HISTORY_DAYS: int = 200  # 最小200交易日
MIN_VALID_STOCKS: int = 80   # 最低有效股票数阈值


@dataclass(frozen=True)
class StockInfo:
    """单只股票信息"""
    symbol: str
    close: float
    sma200: float
    deviation: float


@dataclass(frozen=True)
class InvalidStock:
    """无效股票信息"""
    symbol: str
    reason: str


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

    Args:
        data: yfinance下载的原始数据

    Returns:
        Close价格DataFrame，列为股票代码，行为日期
    """
    # 情况1: MultiIndex (ticker, field)
    if isinstance(data.columns, pd.MultiIndex):
        if 'Close' in data.columns.get_level_values(1):
            result = data.xs('Close', axis=1, level=1)
        elif 'close' in data.columns.get_level_values(1):
            result = data.xs('close', axis=1, level=1)
        else:
            # 尝试直接通过第一层提取
            first_level = list(data.columns.get_level_values(0))
            if 'Close' in first_level:
                result = data['Close']
            elif 'close' in first_level:
                result = data['close']
            else:
                raise ValueError(f"无法解析MultiIndex数据: {data.columns[:5]}")

        # 清理列名：如果是元组，转为简单字符串
        if isinstance(result.columns, pd.MultiIndex):
            result.columns = [c[0] if isinstance(c, tuple) else c for c in result.columns]
        return result

    # 情况2: 普通DataFrame，直接返回Close列
    if 'Close' in data.columns:
        return data[['Close']]

    # 情况3: 单列（单个股票下载）
    if len(data.columns) == 1:
        return data

    raise ValueError(f"无法解析数据列结构: {data.columns[:5]}")


def download_stock_data(
    symbols: List[str],
    as_of_date: Optional[datetime] = None,
    lookback_days: int = 500
) -> pd.DataFrame:
    """
    下载股票历史数据

    Args:
        symbols: 股票代码列表
        as_of_date: 计算日期 (None表示最新数据)
        lookback_days: 回看天数

    Returns:
        DataFrame with columns for each symbol's Close
    """
    end_date = as_of_date or datetime.now()
    start_date = end_date - timedelta(days=lookback_days)

    print(f"📥 下载 {len(symbols)} 只股票历史数据...")
    print(f"   日期范围: {start_date.date()} ~ {end_date.date()}")

    data = yf.download(
        tickers=symbols,
        start=start_date.strftime('%Y-%m-%d'),
        end=end_date.strftime('%Y-%m-%d'),
        interval="1d",
        auto_adjust=True,
        threads=True,
        group_by="ticker",
        progress=True
    )

    # 验证下载结果
    if data.empty:
        raise ValueError("数据下载失败: 返回空DataFrame")

    # 提取Close价格矩阵（兼容不同yfinance版本）
    close_matrix = _extract_close_matrix(data)

    print(f"✅ 下载完成: {close_matrix.shape[0]} 个交易日, {close_matrix.shape[1]} 只股票")

    # 检查是否有足够的交易日计算SMA200
    if close_matrix.shape[0] < MIN_HISTORY_DAYS:
        raise ValueError(
            f"数据不足: 只有 {close_matrix.shape[0]} 个交易日，"
            f"需要至少 {MIN_HISTORY_DAYS} 个交易日计算SMA200"
        )

    return close_matrix


def calculate_sma200_vectorized(close_matrix: pd.DataFrame) -> pd.DataFrame:
    """
    向量化计算200日均线

    Args:
        close_matrix: 列索引为symbol，行索引为date的收盘价矩阵

    Returns:
        SMA200 DataFrame
    """
    return close_matrix.rolling(
        window=MIN_HISTORY_DAYS,
        min_periods=MIN_HISTORY_DAYS
    ).mean()


def _parse_date(date_str: str) -> datetime:
    """
    解析并验证日期字符串

    Args:
        date_str: YYYY-MM-DD 格式日期字符串

    Returns:
        datetime 对象

    Raises:
        ValueError: 日期格式无效
    """
    try:
        parsed = datetime.strptime(date_str, '%Y-%m-%d')
        return parsed
    except ValueError:
        raise ValueError(f"无效日期格式: {date_str}，请使用 YYYY-MM-DD 格式")


def calculate_breadth(
    close_matrix: pd.DataFrame,
    as_of_date: Optional[str] = None
) -> BreadthResult:
    """
    计算NASDAQ-100 200日均线宽度指标

    使用向量化计算，确保所有股票使用同一天的收盘价

    Args:
        close_matrix: 收盘价矩阵
        as_of_date: 计算日期 (YYYY-MM-DD格式), None表示最新

    Returns:
        BreadthResult 对象

    Raises:
        ValueError: 日期无效或数据不足
    """
    symbols: List[str] = list(close_matrix.columns)

    # 解析并验证日期
    if as_of_date is not None:
        as_of_date_str = as_of_date
        _parse_date(as_of_date_str)  # 验证格式
    else:
        as_of_date_str = close_matrix.index.max().strftime('%Y-%m-%d')

    # 获取as_of_date的数据（向前找到最近的交易日）
    available_dates = close_matrix.index
    target_date = None

    for date in reversed(available_dates):
        if date.strftime('%Y-%m-%d') <= as_of_date_str:
            target_date = date
            break

    if target_date is None:
        raise ValueError(f"没有找到 {as_of_date_str} 之前的数据")

    print(f"📅 计算日期: {target_date.date()}")

    # 提取目标日期的收盘价
    close_at_date = close_matrix.loc[target_date]

    # 计算SMA200
    sma200_matrix = calculate_sma200_vectorized(close_matrix)

    # 获取目标日期的SMA200（需要至少200天数据）
    sma200_at_date = sma200_matrix.loc[target_date]

    # 判断是否突破均线 (Close > SMA200)
    above_mask = close_at_date > sma200_at_date
    valid_mask = ~sma200_at_date.isna() & ~close_at_date.isna()

    # 统计
    above_count: int = int(above_mask[valid_mask].sum())
    valid_count: int = int(valid_mask.sum())
    below_count: int = valid_count - above_count

    # 记录无效股票
    symbols_invalid: List[InvalidStock] = []
    for sym in symbols:
        if pd.isna(sma200_at_date[sym]):
            symbols_invalid.append(InvalidStock(sym, "数据不足200天"))
        elif pd.isna(close_at_date[sym]):
            symbols_invalid.append(InvalidStock(sym, "当日无数据"))

    # 记录突破/跌破股票详情
    symbols_above: List[StockInfo] = []
    symbols_below: List[StockInfo] = []

    for sym in symbols:
        if not valid_mask[sym]:
            continue

        close = close_at_date[sym]
        sma = sma200_at_date[sym]
        deviation = (close - sma) / sma * 100

        stock_info = StockInfo(
            symbol=sym,
            close=float(close),
            sma200=float(sma),
            deviation=float(deviation)
        )

        if above_mask[sym]:
            symbols_above.append(stock_info)
        else:
            symbols_below.append(stock_info)

    # 计算宽度百分比
    if valid_count == 0:
        raise ValueError("没有有效股票，无法计算宽度指标")
    breadth_pct: float = above_count / valid_count * 100

    # 检查有效股票数
    if valid_count < MIN_VALID_STOCKS:
        print(f"⚠️ 警告: 有效股票数({valid_count})低于阈值({MIN_VALID_STOCKS})")

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


def print_report(result: BreadthResult) -> None:
    """打印结果报告"""
    print("\n" + "=" * 60)
    print("📊 NASDAQ-100 200日均线宽度指标")
    print("=" * 60)
    print(f"📅 交易日: {result.trade_date}")
    print(f"📈 指标数值: {result.breadth_pct}")
    print("-" * 60)
    print(f"✅ 在均线上: {result.above_200ma} 只")
    print(f"❌ 在均线下: {result.below_200ma} 只")
    print(f"⚠️ 无效股票: {result.invalid_stocks} 只")
    print(f"📋 有效总数: {result.valid_stocks} / {result.total_constituents}")
    print("-" * 60)

    # 显示部分突破股票
    if result.symbols_above:
        print("\n🚀 突破均线 (Top 5):")
        sorted_above = sorted(result.symbols_above, key=lambda x: x.deviation, reverse=True)[:5]
        for s in sorted_above:
            print(f"   {s.symbol:6} {s.close:8.2f} / SMA200={s.sma200:8.2f} (+{s.deviation:.1f}%)")

    if result.symbols_below:
        print("\n📉 跌破均线 (Bottom 5):")
        sorted_below = sorted(result.symbols_below, key=lambda x: x.deviation)[:5]
        for s in sorted_below:
            print(f"   {s.symbol:6} {s.close:8.2f} / SMA200={s.sma200:8.2f} ({s.deviation:.1f}%)")

    print("=" * 60)


def main(as_of_date: Optional[str] = None) -> BreadthResult:
    """
    主函数

    Args:
        as_of_date: 计算日期 (YYYY-MM-DD格式), None表示最新数据

    Returns:
        BreadthResult 对象

    Raises:
        ValueError: 日期无效或数据不足
    """
    print("🚀 NASDAQ-100 200日均线宽度指标计算")
    print("=" * 60)

    # 0. 动态获取成分股
    print("📋 获取NASDAQ-100成分股列表...")
    try:
        symbols = resolve_nasdaq_100_symbols()
        print(f"   ✅ 成功获取 {len(symbols)} 只成分股")
    except Exception as e:
        print(f"   ⚠️ 动态获取失败: {e}")
        from constituents import NASDAQ_100_FALLBACK
        symbols = NASDAQ_100_FALLBACK
        print(f"   🔄 使用备用列表 {len(symbols)} 只")

    # 解析日期参数
    if as_of_date is not None:
        parsed_date = _parse_date(as_of_date)
        end_date = parsed_date + timedelta(days=1)  # 包含目标日期
    else:
        end_date = None

    # 1. 下载数据（如果指定日期，需要更长的回看期）
    lookback_days = 500 if as_of_date is None else 700  # 历史查询需要更多数据
    close_matrix = download_stock_data(
        symbols,
        as_of_date=end_date,
        lookback_days=lookback_days
    )

    # 2. 计算宽度指标
    result = calculate_breadth(close_matrix, as_of_date)

    # 3. 打印报告
    print_report(result)

    return result


if __name__ == "__main__":
    import sys

    # 支持命令行参数指定日期
    date_arg: Optional[str] = sys.argv[1] if len(sys.argv) > 1 else None
    main(date_arg)
