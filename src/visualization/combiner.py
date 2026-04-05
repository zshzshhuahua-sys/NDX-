"""
组合图表生成器

将主图和行业副图合成为单一图表
"""

from __future__ import annotations

from pathlib import Path
from typing import List, Optional, Union

import plotly.graph_objects as go
from plotly.subplots import make_subplots

from .chart_config import COLORS, LAYOUT_CONFIG, SECTOR_COLORS
from .data_loader import BreadthDataPoint, ChartData, NDXDataPoint, SectorDataPoint
from .main_chart import create_main_chart
from .sector_chart import create_sector_chart


def create_combined_chart(
    data: ChartData,
    title: Optional[str] = None,
    width: int = LAYOUT_CONFIG["width"],
    height: int = LAYOUT_CONFIG["height"],
) -> go.Figure:
    """
    创建组合图表 (主图 + 行业副图)

    Args:
        data: 图表数据
        title: 图表标题
        width: 图表宽度
        height: 图表高度

    Returns:
        Plotly Figure 对象
    """
    if title is None:
        start_str = data.start_date.strftime("%Y-%m-%d") if data.start_date else "N/A"
        end_str = data.end_date.strftime("%Y-%m-%d") if data.end_date else "N/A"
        title = f"NASDAQ-100 200日均线宽度指标 ({start_str} ~ {end_str})"

    # 创建子图布局: 2行
    # 第一行: 主图 (宽度 + NDX)
    # 第二行: 行业柱状图
    fig = make_subplots(
        rows=2,
        cols=1,
        row_heights=[LAYOUT_CONFIG["main_height_ratio"], 1 - LAYOUT_CONFIG["main_height_ratio"]],
        specs=[
            [{"secondary_y": True}],  # 主图带双Y轴
            [{}],                     # 副图普通柱状图
        ],
        vertical_spacing=0.15,
    )

    # === 主图: 宽度指标折线 ===
    if data.breadth_history:
        fig.add_trace(
            go.Scatter(
                x=[d.trade_date for d in data.breadth_history],
                y=[d.breadth_pct for d in data.breadth_history],
                name="宽度指标 (%)",
                mode="lines",
                line=dict(color=COLORS["primary"], width=2),
                fill="tozeroy",
                fillcolor="rgba(37, 99, 235, 0.1)",
            ),
            row=1, col=1,
            secondary_y=False,
        )

    # === 主图: NDX 指数折线 ===
    if data.ndx_history:
        fig.add_trace(
            go.Scatter(
                x=[d.trade_date for d in data.ndx_history],
                y=[d.close for d in data.ndx_history],
                name="NDX 指数",
                mode="lines",
                line=dict(color=COLORS["secondary"], width=1.5),
            ),
            row=1, col=1,
            secondary_y=True,
        )

    # 主图 50% 参考线
    fig.add_hline(
        y=50,
        line_dash="dash",
        line_color=COLORS["neutral"],
        line_width=1,
        row=1, col=1,
    )

    # === 副图: 行业柱状图 ===
    if data.sectors:
        sorted_sectors = sorted(data.sectors, key=lambda s: s.breadth_pct, reverse=True)
        colors = [
            SECTOR_COLORS.get(s.sector_code, COLORS["neutral"])
            for s in sorted_sectors
        ]

        fig.add_trace(
            go.Bar(
                x=[s.sector_code for s in sorted_sectors],
                y=[s.breadth_pct for s in sorted_sectors],
                marker_color=colors,
                text=[f"{s.breadth_pct:.0f}%" for s in sorted_sectors],
                textposition="outside",
                name="行业宽度",
                showlegend=False,
            ),
            row=2, col=1,
        )

        # 副图 50% 参考线
        fig.add_hline(
            y=50,
            line_dash="dash",
            line_color=COLORS["neutral"],
            line_width=1,
            row=2, col=1,
        )

    # 布局设置
    fig.update_layout(
        title=dict(
            text=title,
            font=dict(size=LAYOUT_CONFIG["title_font_size"]),
            x=0.5,
            xanchor="center",
        ),
        hovermode="x unified",
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.01,
            xanchor="right",
            x=1,
        ),
        plot_bgcolor=COLORS["background"],
        paper_bgcolor=COLORS["background"],
        font=dict(family=LAYOUT_CONFIG["font_family"]),
        width=width,
        height=height,
    )

    # 主图 Y 轴设置
    fig.update_yaxes(
        title_text="宽度指标 (%)",
        range=[0, 100],
        gridcolor=COLORS["grid"],
        row=1, col=1,
        secondary_y=False,
    )
    fig.update_yaxes(
        title_text="NDX 指数",
        gridcolor=COLORS["grid"],
        row=1, col=1,
        secondary_y=True,
    )

    # 副图 Y 轴设置
    fig.update_yaxes(
        title_text="行业宽度 (%)",
        range=[0, 105],
        gridcolor=COLORS["grid"],
        row=2, col=1,
    )

    # X 轴设置
    fig.update_xaxes(
        title_text="日期",
        gridcolor=COLORS["grid"],
        row=1, col=1,
    )
    fig.update_xaxes(
        title_text="行业",
        tickangle=-45,
        gridcolor=COLORS["grid"],
        row=2, col=1,
    )

    return fig


def save_chart(
    fig: go.Figure,
    output_path: Union[str, Path],
    format: str = "png",
) -> Path:
    """
    保存图表到文件

    Args:
        fig: Plotly Figure 对象
        output_path: 输出文件路径
        format: 输出格式 (png/html/svg)

    Returns:
        输出文件路径
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if format == "html":
        fig.write_html(output_path)
    elif format in ("png", "jpg", "webp"):
        # 需要 kaleido 进行静态导出
        try:
            fig.write_image(output_path, format=format)
        except Exception as exc:
            raise RuntimeError(
                f"Failed to export {format}. "
                "Install kaleido: pip install kaleido"
            ) from exc
    else:
        raise ValueError(f"Unsupported format: {format}")

    return output_path
