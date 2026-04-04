"""
NASDAQ-100 成分股行业分类映射（备用方案）

当 Finnhub API 不可用时，使用硬编码的行业映射
数据来源：基于公开的 NASDAQ-100 板块分类
注意：每个股票只出现一次
"""

from __future__ import annotations

from typing import Dict

# NASDAQ-100 成分股行业映射
# 格式: symbol -> (sector, sector_code, industry)
# 每个股票只定义一次
NASDAQ100_SECTOR_MAPPING: Dict[str, Dict[str, str]] = {
    # ==================== Technology ====================
    "AAPL": {"sector": "Technology", "sector_code": "TECH", "industry": "Consumer Electronics"},
    "ADBE": {"sector": "Technology", "sector_code": "TECH", "industry": "Software"},
    "ADI": {"sector": "Technology", "sector_code": "TECH", "industry": "Semiconductors"},
    "AMAT": {"sector": "Technology", "sector_code": "TECH", "industry": "Semiconductor Equipment"},
    "ARM": {"sector": "Technology", "sector_code": "TECH", "industry": "Semiconductors"},
    "AVGO": {"sector": "Technology", "sector_code": "TECH", "industry": "Semiconductors"},
    "CDNS": {"sector": "Technology", "sector_code": "TECH", "industry": "Software"},
    "CDW": {"sector": "Technology", "sector_code": "TECH", "industry": "Technology Services"},
    "CRWD": {"sector": "Technology", "sector_code": "TECH", "industry": "Software"},
    "CSX": {"sector": "Technology", "sector_code": "TECH", "industry": "Railroads"},
    "CTSH": {"sector": "Technology", "sector_code": "TECH", "industry": "IT Services"},
    "DDOG": {"sector": "Technology", "sector_code": "TECH", "industry": "Software"},
    "EA": {"sector": "Technology", "sector_code": "TECH", "industry": "Interactive Entertainment"},
    "FTNT": {"sector": "Technology", "sector_code": "TECH", "industry": "Software"},
    "GFS": {"sector": "Technology", "sector_code": "TECH", "industry": "Semiconductors"},
    "GOOG": {"sector": "Communication Services", "sector_code": "COMMS", "industry": "Internet Services"},
    "GOOGL": {"sector": "Communication Services", "sector_code": "COMMS", "industry": "Internet Services"},
    "INTC": {"sector": "Technology", "sector_code": "TECH", "industry": "Semiconductors"},
    "INTU": {"sector": "Technology", "sector_code": "TECH", "industry": "Software"},
    "JBL": {"sector": "Technology", "sector_code": "TECH", "industry": "Electronic Components"},
    "JD": {"sector": "Consumer Discretionary", "sector_code": "CONS_DISC", "industry": "Internet Retail"},
    "KLAC": {"sector": "Technology", "sector_code": "TECH", "industry": "Semiconductor Equipment"},
    "LRCX": {"sector": "Technology", "sector_code": "TECH", "industry": "Semiconductor Equipment"},
    "MCHP": {"sector": "Technology", "sector_code": "TECH", "industry": "Semiconductors"},
    "META": {"sector": "Communication Services", "sector_code": "COMMS", "industry": "Social Media"},
    "MSFT": {"sector": "Technology", "sector_code": "TECH", "industry": "Software"},
    "MU": {"sector": "Technology", "sector_code": "TECH", "industry": "Semiconductors"},
    "NFLX": {"sector": "Communication Services", "sector_code": "COMMS", "industry": "Streaming"},
    "NVDA": {"sector": "Technology", "sector_code": "TECH", "industry": "Semiconductors"},
    "NXPI": {"sector": "Technology", "sector_code": "TECH", "industry": "Semiconductors"},
    "ON": {"sector": "Technology", "sector_code": "TECH", "industry": "Semiconductors"},
    "PANW": {"sector": "Technology", "sector_code": "TECH", "industry": "Software"},
    "PAYX": {"sector": "Technology", "sector_code": "TECH", "industry": "Business Services"},
    "PCAR": {"sector": "Technology", "sector_code": "TECH", "industry": "Heavy Machinery"},
    "PYPL": {"sector": "Financials", "sector_code": "FIN", "industry": "Payments"},
    "QCOM": {"sector": "Technology", "sector_code": "TECH", "industry": "Semiconductors"},
    "ROP": {"sector": "Technology", "sector_code": "TECH", "industry": "Software"},
    "SNPS": {"sector": "Technology", "sector_code": "TECH", "industry": "Software"},
    "STX": {"sector": "Technology", "sector_code": "TECH", "industry": "Computer Hardware"},
    "TEAM": {"sector": "Technology", "sector_code": "TECH", "industry": "Software"},
    "TMUS": {"sector": "Communication Services", "sector_code": "COMMS", "industry": "Telecom"},
    "TTD": {"sector": "Technology", "sector_code": "TECH", "industry": "Software"},
    "TTWO": {"sector": "Technology", "sector_code": "TECH", "industry": "Interactive Entertainment"},
    "TXN": {"sector": "Technology", "sector_code": "TECH", "industry": "Semiconductors"},
    "UBER": {"sector": "Technology", "sector_code": "TECH", "industry": "Software"},
    "WDAY": {"sector": "Technology", "sector_code": "TECH", "industry": "Software"},
    "WDC": {"sector": "Technology", "sector_code": "TECH", "industry": "Computer Hardware"},
    "ZS": {"sector": "Technology", "sector_code": "TECH", "industry": "Software"},

    # ==================== Healthcare ====================
    "AMGN": {"sector": "Healthcare", "sector_code": "HEALTH", "industry": "Biotechnology"},
    "DXCM": {"sector": "Healthcare", "sector_code": "HEALTH", "industry": "Medical Devices"},
    "GEHC": {"sector": "Healthcare", "sector_code": "HEALTH", "industry": "Medical Devices"},
    "GILD": {"sector": "Healthcare", "sector_code": "HEALTH", "industry": "Biotechnology"},
    "IDXX": {"sector": "Healthcare", "sector_code": "HEALTH", "industry": "Diagnostics"},
    "ISRG": {"sector": "Healthcare", "sector_code": "HEALTH", "industry": "Medical Devices"},
    "MOH": {"sector": "Healthcare", "sector_code": "HEALTH", "industry": "Healthcare Plans"},
    "MRNA": {"sector": "Healthcare", "sector_code": "HEALTH", "industry": "Biotechnology"},
    "PTC": {"sector": "Technology", "sector_code": "TECH", "industry": "Software"},
    "REGN": {"sector": "Healthcare", "sector_code": "HEALTH", "industry": "Biotechnology"},
    "VEEV": {"sector": "Healthcare", "sector_code": "HEALTH", "industry": "Healthcare Technology"},
    "VRTX": {"sector": "Healthcare", "sector_code": "HEALTH", "industry": "Biotechnology"},

    # ==================== Consumer Discretionary ====================
    "ABNB": {"sector": "Consumer Discretionary", "sector_code": "CONS_DISC", "industry": "Travel"},
    "AMZN": {"sector": "Consumer Discretionary", "sector_code": "CONS_DISC", "industry": "Internet Retail"},
    "BKNG": {"sector": "Consumer Discretionary", "sector_code": "CONS_DISC", "industry": "Travel Services"},
    "DASH": {"sector": "Consumer Discretionary", "sector_code": "CONS_DISC", "industry": "Food Delivery"},
    "DLTR": {"sector": "Consumer Discretionary", "sector_code": "CONS_DISC", "industry": "Retail"},
    "LULU": {"sector": "Consumer Discretionary", "sector_code": "CONS_DISC", "industry": "Apparel"},
    "MAR": {"sector": "Consumer Discretionary", "sector_code": "CONS_DISC", "industry": "Hotels"},
    "ORLY": {"sector": "Consumer Discretionary", "sector_code": "CONS_DISC", "industry": "Retail"},
    "ROST": {"sector": "Consumer Discretionary", "sector_code": "CONS_DISC", "industry": "Retail"},
    "SBUX": {"sector": "Consumer Discretionary", "sector_code": "CONS_DISC", "industry": "Restaurants"},
    "ULTA": {"sector": "Consumer Discretionary", "sector_code": "CONS_DISC", "industry": "Retail"},

    # ==================== Communication Services ====================
    "CHTR": {"sector": "Communication Services", "sector_code": "COMMS", "industry": "Telecom"},

    # ==================== Consumer Staples ====================
    "COST": {"sector": "Consumer Staples", "sector_code": "CONS_STAP", "industry": "Retail"},
    "KDP": {"sector": "Consumer Staples", "sector_code": "CONS_STAP", "industry": "Beverages"},
    "KHC": {"sector": "Consumer Staples", "sector_code": "CONS_STAP", "industry": "Food"},
    "MDLZ": {"sector": "Consumer Staples", "sector_code": "CONS_STAP", "industry": "Food"},
    "MNST": {"sector": "Consumer Staples", "sector_code": "CONS_STAP", "industry": "Beverages"},
    "PM": {"sector": "Consumer Staples", "sector_code": "CONS_STAP", "industry": "Tobacco"},

    # ==================== Energy ====================
    "BKR": {"sector": "Energy", "sector_code": "ENERGY", "industry": "Oil & Gas"},
    "FANG": {"sector": "Energy", "sector_code": "ENERGY", "industry": "Oil & Gas"},

    # ==================== Financials ====================
    "AXON": {"sector": "Financials", "sector_code": "FIN", "industry": "Aerospace"},
    "COIN": {"sector": "Financials", "sector_code": "FIN", "industry": "Financial Services"},
    "CPRT": {"sector": "Financials", "sector_code": "FIN", "industry": "Financial Services"},
    "ICE": {"sector": "Financials", "sector_code": "FIN", "industry": "Financial Services"},
    "MS": {"sector": "Financials", "sector_code": "FIN", "industry": "Investment Banking"},
    "MTB": {"sector": "Financials", "sector_code": "FIN", "industry": "Banking"},
    "SPGI": {"sector": "Financials", "sector_code": "FIN", "industry": "Financial Services"},
    "TFC": {"sector": "Financials", "sector_code": "FIN", "industry": "Banking"},
    "USB": {"sector": "Financials", "sector_code": "FIN", "industry": "Banking"},
    "WFC": {"sector": "Financials", "sector_code": "FIN", "industry": "Banking"},

    # ==================== Industrials ====================
    "CAT": {"sector": "Industrials", "sector_code": "INDUST", "industry": "Heavy Machinery"},
    "CTAS": {"sector": "Industrials", "sector_code": "INDUST", "industry": "Services"},
    "DE": {"sector": "Industrials", "sector_code": "INDUST", "industry": "Heavy Machinery"},
    "ETN": {"sector": "Industrials", "sector_code": "INDUST", "industry": "Electrical Equipment"},
    "FAST": {"sector": "Industrials", "sector_code": "INDUST", "industry": "Industrial Distribution"},
    "GD": {"sector": "Industrials", "sector_code": "INDUST", "industry": "Aerospace"},
    "HON": {"sector": "Industrials", "sector_code": "INDUST", "industry": "Conglomerates"},
    "NSC": {"sector": "Industrials", "sector_code": "INDUST", "industry": "Railroads"},
    "ODFL": {"sector": "Industrials", "sector_code": "INDUST", "industry": "Trucking"},
    "PH": {"sector": "Industrials", "sector_code": "INDUST", "industry": "Electrical Equipment"},
    "RTX": {"sector": "Industrials", "sector_code": "INDUST", "industry": "Aerospace"},
    "UNP": {"sector": "Industrials", "sector_code": "INDUST", "industry": "Railroads"},
    "UAL": {"sector": "Industrials", "sector_code": "INDUST", "industry": "Airlines"},

    # ==================== Materials ====================
    "LIN": {"sector": "Materials", "sector_code": "MAT", "industry": "Chemicals"},
    "NEM": {"sector": "Materials", "sector_code": "MAT", "industry": "Mining"},

    # ==================== Real Estate ====================
    "PLD": {"sector": "Real Estate", "sector_code": "REIT", "industry": "REIT"},

    # ==================== Utilities ====================
    "AEP": {"sector": "Utilities", "sector_code": "UTIL", "industry": "Regulated Electric"},
    "CEG": {"sector": "Utilities", "sector_code": "UTIL", "industry": "Utilities"},
    "EXC": {"sector": "Utilities", "sector_code": "UTIL", "industry": "Utilities"},
    "XEL": {"sector": "Utilities", "sector_code": "UTIL", "industry": "Utilities"},
    "WEC": {"sector": "Utilities", "sector_code": "UTIL", "industry": "Utilities"},

    # ==================== 补充缺失股票 (2026-04-04) ====================
    # ADP - Automatic Data Processing
    "ADP": {"sector": "Technology", "sector_code": "TECH", "industry": "Software - Application"},
    # ADSK - Autodesk
    "ADSK": {"sector": "Technology", "sector_code": "TECH", "industry": "Software - Application"},
    # ANET - Arista Networks
    "ANET": {"sector": "Technology", "sector_code": "TECH", "industry": "Computer Hardware"},
    # CMCSA - Comcast
    "CMCSA": {"sector": "Communication Services", "sector_code": "COMMS", "industry": "Telecom Services"},
    # LCTC - Lucid Group (原 Lucid Motors)
    "LCTC": {"sector": "Technology", "sector_code": "TECH", "industry": "Scientific & Technical Instruments"},
    # LU - Lufax Holding
    "LU": {"sector": "Financials", "sector_code": "FIN", "industry": "Credit Services"},
    # MRVL - Marvell Technology
    "MRVL": {"sector": "Technology", "sector_code": "TECH", "industry": "Semiconductors"},
    # PDD - PDD Holdings (Pinduoduo)
    "PDD": {"sector": "Consumer Discretionary", "sector_code": "CONS_DISC", "industry": "Internet Retail"},
    # PLTR - Palantir Technologies
    "PLTR": {"sector": "Technology", "sector_code": "TECH", "industry": "Software - Infrastructure"},
    # SMCI - Super Micro Computer
    "SMCI": {"sector": "Technology", "sector_code": "TECH", "industry": "Computer Hardware"},
}


def get_fallback_sector(symbol: str) -> Dict[str, str]:
    """
    获取股票的行业分类（硬编码备用）

    Args:
        symbol: 股票代码

    Returns:
        包含 sector, sector_code, industry 的字典
        如果未找到，返回默认值
    """
    return NASDAQ100_SECTOR_MAPPING.get(symbol, {
        "sector": "Unknown",
        "sector_code": "OTHER",
        "industry": "Unknown"
    })
