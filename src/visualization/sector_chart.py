"""
行业柱状图生成器

生成各行业宽度指标的柱状图
"""

from __future__ import annotations

from typing import List, Optional

import plotly.graph_objects as go

from .chart_config import COLORS, SECTOR_COLORS, LAYOUT_CONFIG
from .data_loader import SectorDataPoint


def create_sector_chart(
    sectors: List[SectorDataPoint],
    title: Optional[str] = None,
) -> go.Figure:
    """
    创建行业宽度柱状图

    Args:
        sectors: 行业数据列表
        title: 图表标题

    Returns:
        Plotly Figure 对象
    """
    if title is None:
        title = "行业宽度指标"

    if not sectors:
        # 返回空图表
        fig = go.Figure()
        fig.update_layout(title=title)
        return fig

    # 按 breadth_pct 降序排列
    sorted_sectors = sorted(sectors, key=lambda s: s.breadth_pct, reverse=True)

    # 生成颜色
    colors = [
        SECTOR_COLORS.get(s.sector_code, COLORS["neutral"])
        for s in sorted_sectors
    ]

    # 创建柱状图
    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=[s.sector_code for s in sorted_sectors],
        y=[s.breadth_pct for s in sorted_sectors],
        marker_color=colors,
        text=[f"{s.breadth_pct:.0f}%" for s in sorted_sectors],
        textposition="outside",
        name="行业宽度",
        hovertemplate=(
            "<b>%{x}</b><br>"
            "宽度: %{y:.1f}%<br>"
            "均线上方: %{customdata[0]}/{customdata[1]}<extra></extra>"
        ),
        customdata=[[s.above_200ma, s.total_stocks] for s in sorted_sectors],
    ))

    # 添加 50% 参考线
    fig.add_hline(
        y=50,
        line_dash="dash",
        line_color=COLORS["neutral"],
        line_width=1,
    )

    # 布局设置
    fig.update_layout(
        title=dict(
            text=title,
            font=dict(size=LAYOUT_CONFIG["font_size"]),
        ),
        xaxis_title="行业",
        yaxis=dict(
            title="宽度指标 (%)",
            range=[0, 105],
            gridcolor=COLORS["grid"],
        ),
        showlegend=False,
        plot_bgcolor=COLORS["background"],
        paper_bgcolor=COLORS["background"],
        font=dict(family=LAYOUT_CONFIG["font_family"]),
        margin=dict(t=40, b=60),
    )

    # X 轴设置
    fig.update_xaxes(
        tickangle=-45,
        gridcolor=COLORS["grid"],
    )

    return fig
