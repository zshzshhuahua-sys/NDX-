"""
图表配置常量
"""

from typing import Dict

# 颜色主题
COLORS: Dict[str, str] = {
    # 主色调
    "primary": "#2563eb",        # 蓝色 - 宽度指标线
    "secondary": "#9333ea",      # 紫色 - NDX 指数线
    "positive": "#16a34a",       # 绿色 - 50%以上
    "negative": "#dc2626",       # 红色 - 50%以下
    "neutral": "#6b7280",       # 灰色 - 参考线
    "background": "#ffffff",     # 白色背景
    "grid": "#e5e7eb",           # 网格线
    "text": "#1f2937",           # 主文本
}

# 行业颜色映射
SECTOR_COLORS: Dict[str, str] = {
    "TECH": "#2563eb",       # 科技 - 蓝
    "HEALTH": "#16a34a",    # 医疗 - 绿
    "CONS_DISC": "#f59e0b", # 消费 - 橙
    "COMMS": "#8b5cf6",      # 通信 - 紫
    "CONS_STAP": "#84cc16",  # 必需消费 - 黄绿
    "ENERGY": "#f97316",     # 能源 - 深橙
    "FIN": "#06b6d4",        # 金融 - 青
    "INDUST": "#71717a",     # 工业 - 灰
    "MAT": "#a1887f",        # 材料 - 棕
    "REIT": "#ec4899",       # 房地产 - 粉
    "UTIL": "#22c55e",       # 公用事业 - 亮绿
    "OTHER": "#6b7280",      # 其他 - 灰
}

# 布局配置
LAYOUT_CONFIG: Dict[str, any] = {
    "width": 1200,
    "height": 800,
    "main_height_ratio": 0.6,  # 主图占60%
    "font_family": "Arial, sans-serif",
    "font_size": 12,
    "title_font_size": 18,
}

# 输出配置
OUTPUT_CONFIG: Dict[str, str] = {
    "default_format": "png",
    "default_output_dir": "output",
    "filename_pattern": "breadth_{date}.{format}",
}
