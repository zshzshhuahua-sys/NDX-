"""
主图生成器

生成宽度指标 + NDX 指数的组合图表
"""

from __future__ import annotations

from typing import List, Optional

import plotly.graph_objects as go
from plotly.subplots import make_subplots

from .chart_config import COLORS, LAYOUT_CONFIG
from .data_loader import BreadthDataPoint, NDXDataPoint


def create_main_chart(
    breadth_data: List[BreadthDataPoint],
    ndx_data: List[NDXDataPoint],
    title: Optional[str] = None,
    show_legend: bool = True,
) -> go.Figure:
    """
    创建主图: 宽度线 + NDX 指数线

    Args:
        breadth_data: 宽度历史数据
        ndx_data: NDX 指数历史数据
        title: 图表标题
        show_legend: 是否显示图例

    Returns:
        Plotly Figure 对象
    """
    if title is None:
        title = "NASDAQ-100 200日均线宽度指标"

    # 创建带双Y轴的图表
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # 宽度指标折线
    if breadth_data:
        fig.add_trace(
            go.Scatter(
                x=[d.trade_date for d in breadth_data],
                y=[d.breadth_pct for d in breadth_data],
                name="宽度指标 (%)",
                mode="lines",
                line=dict(color=COLORS["primary"], width=2),
                fill="tozeroy",
                fillcolor=f"rgba(37, 99, 235, 0.1)",
            ),
            secondary_y=False,
        )

    # NDX 指数折线
    if ndx_data:
        fig.add_trace(
            go.Scatter(
                x=[d.trade_date for d in ndx_data],
                y=[d.close for d in ndx_data],
                name="NDX 指数",
                mode="lines",
                line=dict(color=COLORS["secondary"], width=1.5),
            ),
            secondary_y=True,
        )

    # 50% 参考线
    fig.add_hline(
        y=50,
        line_dash="dash",
        line_color=COLORS["neutral"],
        line_width=1,
        annotation_text="50% 临界",
        annotation_position="bottom right",
    )

    # 布局设置
    fig.update_layout(
        title=dict(
            text=title,
            font=dict(size=LAYOUT_CONFIG["title_font_size"]),
        ),
        xaxis_title="日期",
        yaxis=dict(
            title="宽度指标 (%)",
            range=[0, 100],
            gridcolor=COLORS["grid"],
        ),
        yaxis2=dict(
            title="NDX 指数",
            gridcolor=COLORS["grid"],
        ),
        hovermode="x unified",
        showlegend=show_legend,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
        ),
        plot_bgcolor=COLORS["background"],
        paper_bgcolor=COLORS["background"],
        font=dict(family=LAYOUT_CONFIG["font_family"]),
    )

    # X 轴格式
    fig.update_xaxes(
        gridcolor=COLORS["grid"],
        showgrid=True,
    )

    return fig
