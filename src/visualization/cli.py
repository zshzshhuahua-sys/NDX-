"""
可视化 CLI

生成 NASDAQ-100 200日均线宽度指标图表
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, timedelta
from logging import getLogger
from pathlib import Path

from .chart_config import LAYOUT_CONFIG, OUTPUT_CONFIG
from .combiner import create_combined_chart, save_chart
from .data_loader import ChartDataLoader


logger = getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description="NASDAQ-100 200日均线宽度指标可视化",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
    python -m visualization                          # 默认过去1年
    python -m visualization --start 2025-01-01       # 指定起始日期
    python -m visualization --end 2025-12-31       # 指定结束日期
    python -m visualization --interactive            # 生成交互式 HTML
    python -m visualization --no-sector              # 隐藏行业副图
    python -m visualization --width 1600 --height 900  # 自定义尺寸
        """
    )

    today = date.today()

    parser.add_argument(
        "--start",
        type=str,
        default=(today - timedelta(days=365)).isoformat(),
        help=f"起始日期 (YYYY-MM-DD, 默认: {(today - timedelta(days=365)).isoformat()})",
    )
    parser.add_argument(
        "--end",
        type=str,
        default=today.isoformat(),
        help=f"结束日期 (YYYY-MM-DD, 默认: {today.isoformat()})",
    )
    parser.add_argument(
        "--output", "-o",
        type=str,
        default=None,
        help="输出文件路径 (默认: 自动生成)",
    )
    parser.add_argument(
        "--format", "-f",
        type=str,
        choices=["png", "html", "svg"],
        default="png",
        help="输出格式 (默认: png)",
    )
    parser.add_argument(
        "--interactive", "-i",
        action="store_true",
        help="生成交互式 HTML (等同于 --format html)",
    )
    parser.add_argument(
        "--no-sector",
        action="store_true",
        help="隐藏行业副图",
    )
    parser.add_argument(
        "--width",
        type=int,
        default=LAYOUT_CONFIG["width"],
        help=f"图表宽度 (默认: {LAYOUT_CONFIG['width']})",
    )
    parser.add_argument(
        "--height",
        type=int,
        default=LAYOUT_CONFIG["height"],
        help=f"图表高度 (默认: {LAYOUT_CONFIG['height']})",
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default=None,
        help="数据目录路径",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="显示详细日志",
    )

    return parser.parse_args()


def parse_date(date_str: str) -> date:
    """解析日期字符串"""
    try:
        return date.fromisoformat(date_str)
    except ValueError:
        raise ValueError(f"无效日期格式: {date_str}，请使用 YYYY-MM-DD 格式")


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
        start_date = parse_date(args.start)
        end_date = parse_date(args.end)
    except ValueError as exc:
        logger.error("%s", exc)
        sys.exit(1)

    if start_date > end_date:
        logger.error("起始日期不能晚于结束日期")
        sys.exit(1)

    # 确定输出格式
    output_format = "html" if args.interactive else args.format

    # 生成输出路径
    if args.output:
        output_path = Path(args.output)
    else:
        output_dir = Path(OUTPUT_CONFIG["default_output_dir"])
        filename = f"breadth_{start_date}_{end_date}.{output_format}"
        output_path = output_dir / filename

    logger.info("加载数据: %s ~ %s", start_date, end_date)

    # 确定数据目录
    if args.data_dir:
        data_dir = Path(args.data_dir)
    else:
        # 默认使用项目 data 目录
        data_dir = Path(__file__).parent.parent.parent / "data"

    # 加载数据
    loader = ChartDataLoader(data_dir=data_dir)
    chart_data = loader.load(
        start_date=start_date,
        end_date=end_date,
        load_sectors=not args.no_sector,
    )

    if not chart_data.breadth_history:
        logger.error("没有找到宽度历史数据")
        logger.info("请先运行 python src/ndx_breadth.py --save 保存数据")
        sys.exit(1)

    logger.info(
        "数据加载完成: %d 天宽度数据, %d 天 NDX 数据, %d 个行业",
        len(chart_data.breadth_history),
        len(chart_data.ndx_history),
        len(chart_data.sectors),
    )

    # 生成图表
    logger.info("生成图表...")
    fig = create_combined_chart(
        data=chart_data,
        width=args.width,
        height=args.height,
    )

    # 保存图表
    try:
        save_chart(fig, output_path, output_format)
        logger.info("图表已保存: %s", output_path)
    except Exception as exc:
        logger.error("保存图表失败: %s", exc)
        if output_format == "png" and "kaleido" in str(exc):
            logger.info("提示: 运行 pip install kaleido 安装导出依赖")
        sys.exit(1)


if __name__ == "__main__":
    main()
