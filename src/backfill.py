"""
历史数据回填脚本

回溯计算历史宽度指标数据
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, timedelta
from logging import getLogger
from pathlib import Path

import pandas as pd
import yfinance as yf

from storage import JsonParquetRepository
from ndx_breadth import calculate_breadth, download_stock_data


logger = getLogger(__name__)


def backfill_history(
    start_date: date,
    end_date: date,
    data_dir: Path,
    dry_run: bool = False,
) -> int:
    """
    回填历史数据

    Args:
        start_date: 起始日期
        end_date: 结束日期
        data_dir: 数据目录
        dry_run: 是否仅模拟运行

    Returns:
        回填的记录数
    """
    repo = JsonParquetRepository(data_dir)

    # 加载已有数据
    existing_records = repo.find_range(start_date, end_date)
    existing_dates = {r.trade_date for r in existing_records}
    logger.info("已有数据: %d 条", len(existing_dates))

    # 生成需要计算的所有交易日
    all_dates = []
    current = start_date
    while current <= end_date:
        # 跳过周末
        if current.weekday() < 5:
            all_dates.append(current)
        current += timedelta(days=1)

    dates_to_calculate = [d for d in all_dates if d not in existing_dates]
    logger.info("需要计算: %d 天", len(dates_to_calculate))

    if not dates_to_calculate:
        logger.info("无需计算，已有的数据覆盖目标范围")
        return 0

    # 获取成分股
    from constituents import HARDCODE_NASDAQ_100
    symbols = HARDCODE_NASDAQ_100

    # 下载完整历史数据（一次性下载，减少 API 调用）
    lookback_days = (end_date - start_date).days + 300  # 多下载一些确保有 200 日均线
    logger.info("下载 %d 只股票历史数据 (回看 %d 天)...", len(symbols), lookback_days)
    close_matrix = download_stock_data(
        symbols=symbols,
        as_of_date=end_date,
        lookback_days=lookback_days,
    )

    count = 0
    for target_date in dates_to_calculate:
        date_str = target_date.strftime("%Y-%m-%d")

        try:
            # 计算宽度指标
            result = calculate_breadth(close_matrix, date_str)

            if not dry_run:
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
                    constituents_source="hardcode",
                    data_source="yfinance",
                )

            count += 1
            logger.info("已计算: %s (宽度: %.1f%%)", date_str, result.breadth_pct)

        except Exception as exc:
            logger.warning("计算失败 %s: %s", date_str, exc)

    logger.info("完成! 共回填 %d 条记录", count)
    return count


def parse_args() -> argparse.Namespace:
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description="回填历史宽度指标数据",
    )

    today = date.today()

    parser.add_argument(
        "--start",
        type=str,
        default=(today - timedelta(days=365)).isoformat(),
        help=f"起始日期 (默认: 1年前)",
    )
    parser.add_argument(
        "--end",
        type=str,
        default=today.isoformat(),
        help=f"结束日期 (默认: 今天)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="仅模拟运行，不保存数据",
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default=None,
        help="数据目录",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="显示详细日志",
    )

    return parser.parse_args()


def main() -> None:
    """主函数"""
    args = parse_args()

    # 配置日志
    import logging
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        stream=sys.stdout,
    )

    # 解析日期
    try:
        start_date = date.fromisoformat(args.start)
        end_date = date.fromisoformat(args.end)
    except ValueError as exc:
        logger.error("日期格式错误: %s", exc)
        sys.exit(1)

    if start_date > end_date:
        logger.error("起始日期不能晚于结束日期")
        sys.exit(1)

    # 确定数据目录
    if args.data_dir:
        data_dir = Path(args.data_dir)
    else:
        data_dir = Path(__file__).parent.parent / "data"

    logger.info("开始回填: %s ~ %s", start_date, end_date)

    if args.dry_run:
        logger.info("DRY-RUN 模式: 不会保存任何数据")

    backfill_history(
        start_date=start_date,
        end_date=end_date,
        data_dir=data_dir,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
