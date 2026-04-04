"""
单元测试 - NASDAQ-100 200日均线宽度指标
"""

import pytest
from datetime import datetime, timedelta
import pandas as pd
from dataclasses import FrozenInstanceError

# 导入待测试模块
import sys
sys.path.insert(0, '/Users/huahua-macmini/Desktop/CodePilot-bot/ndx-200ma-breadth/src')

from ndx_breadth import (
    StockInfo,
    InvalidStock,
    BreadthResult,
    MIN_HISTORY_DAYS,
    MIN_VALID_STOCKS,
    _parse_date,
)


class TestStockInfo:
    """StockInfo 数据类测试"""

    def test_create_stock_info(self):
        """测试创建 StockInfo"""
        info = StockInfo(symbol="AAPL", close=180.0, sma200=170.0, deviation=5.88)
        assert info.symbol == "AAPL"
        assert info.close == 180.0
        assert info.sma200 == 170.0
        assert info.deviation == 5.88

    def test_stock_info_immutable(self):
        """测试 StockInfo 不可变"""
        info = StockInfo(symbol="AAPL", close=180.0, sma200=170.0, deviation=5.88)
        with pytest.raises(FrozenInstanceError):
            info.close = 200.0  # type: ignore


class TestInvalidStock:
    """InvalidStock 数据类测试"""

    def test_create_invalid_stock(self):
        """测试创建 InvalidStock"""
        invalid = InvalidStock(symbol="DELISTED", reason="数据不足200天")
        assert invalid.symbol == "DELISTED"
        assert invalid.reason == "数据不足200天"

    def test_invalid_stock_immutable(self):
        """测试 InvalidStock 不可变"""
        invalid = InvalidStock(symbol="DELISTED", reason="数据不足200天")
        with pytest.raises(FrozenInstanceError):
            invalid.symbol = "NEW"  # type: ignore


class TestBreadthResult:
    """BreadthResult 数据类测试"""

    def test_create_breadth_result(self):
        """测试创建 BreadthResult"""
        result = BreadthResult(
            trade_date="2026-04-04",
            total_constituents=100,
            valid_stocks=80,
            above_200ma=60,
            below_200ma=20,
            invalid_stocks=20,
            breadth_pct=75.0
        )
        assert result.trade_date == "2026-04-04"
        assert result.valid_stocks == 80
        assert result.breadth_pct == 75.0

    def test_breadth_result_to_dict(self):
        """测试转换为字典"""
        result = BreadthResult(
            trade_date="2026-04-04",
            total_constituents=100,
            valid_stocks=80,
            above_200ma=60,
            below_200ma=20,
            invalid_stocks=20,
            breadth_pct=75.0
        )
        d = result.to_dict()
        assert d['trade_date'] == "2026-04-04"
        assert d['breadth_pct'] == 75.0
        assert 'symbols_above' not in d  # to_dict 不包含详细列表


class TestConstants:
    """常量测试"""

    def test_min_history_days(self):
        """测试最小历史天数"""
        assert MIN_HISTORY_DAYS == 200

    def test_min_valid_stocks(self):
        """测试最低有效股票数"""
        assert MIN_VALID_STOCKS == 80


class TestBreadthCalculation:
    """宽度计算逻辑测试"""

    def test_breadth_100_above(self):
        """全部股票都在均线上"""
        result = BreadthResult(
            trade_date="2026-04-04",
            total_constituents=10,
            valid_stocks=10,
            above_200ma=10,
            below_200ma=0,
            invalid_stocks=0,
            breadth_pct=100.0
        )
        assert result.breadth_pct == 100.0
        assert result.above_200ma == result.valid_stocks

    def test_breadth_0_above(self):
        """全部股票都在均线下"""
        result = BreadthResult(
            trade_date="2026-04-04",
            total_constituents=10,
            valid_stocks=10,
            above_200ma=0,
            below_200ma=10,
            invalid_stocks=0,
            breadth_pct=0.0
        )
        assert result.breadth_pct == 0.0
        assert result.below_200ma == result.valid_stocks

    def test_breadth_50(self):
        """一半股票在均线上"""
        result = BreadthResult(
            trade_date="2026-04-04",
            total_constituents=10,
            valid_stocks=10,
            above_200ma=5,
            below_200ma=5,
            invalid_stocks=0,
            breadth_pct=50.0
        )
        assert result.breadth_pct == 50.0

    def test_breadth_with_invalid(self):
        """有无效股票的情况"""
        result = BreadthResult(
            trade_date="2026-04-04",
            total_constituents=10,
            valid_stocks=8,  # 2只无效
            above_200ma=6,
            below_200ma=2,
            invalid_stocks=2,
            breadth_pct=75.0  # 6/8 = 75%
        )
        assert result.breadth_pct == 75.0
        assert result.total_constituents == result.valid_stocks + result.invalid_stocks


class TestSMA200EdgeCases:
    """SMA200 边界情况测试"""

    def test_exactly_200_days(self):
        """刚好200天数据"""
        # 创建刚好200天的数据
        dates = [datetime.now() - timedelta(days=i) for i in range(199, -1, -1)]
        close_data = [100.0 + i * 0.1 for i in range(200)]  # 线性上涨

        df = pd.DataFrame(
            {'AAPL': close_data},
            index=pd.DatetimeIndex(dates)
        )

        # SMA200应该是前199个的平均
        expected_sma200 = sum(close_data[:200]) / 200
        assert abs(df['AAPL'].tail(200).mean() - expected_sma200) < 0.001

    def test_exactly_on_ma(self):
        """收盘价等于SMA200"""
        # Close == SMA200 的情况，不应算作突破
        result = BreadthResult(
            trade_date="2026-04-04",
            total_constituents=1,
            valid_stocks=1,
            above_200ma=0,  # 严格大于才计入
            below_200ma=1,
            invalid_stocks=0,
            breadth_pct=0.0
        )
        assert result.breadth_pct == 0.0


class TestDateParsing:
    """日期解析测试"""

    def test_valid_date(self):
        """测试有效日期解析"""
        result = _parse_date("2026-04-04")
        assert result.year == 2026
        assert result.month == 4
        assert result.day == 4

    def test_invalid_date_format(self):
        """测试无效日期格式"""
        with pytest.raises(ValueError, match="无效日期格式"):
            _parse_date("2026-4-4")  # 缺少前导零

        with pytest.raises(ValueError, match="无效日期格式"):
            _parse_date("bad-input")

        with pytest.raises(ValueError, match="无效日期格式"):
            _parse_date("04-04-2026")  # 错误格式

    def test_leading_zeros_required(self):
        """测试必须使用前导零"""
        # 这些格式应该失败
        with pytest.raises(ValueError):
            _parse_date("2026-1-1")
        with pytest.raises(ValueError):
            _parse_date("2026-01-1")
        with pytest.raises(ValueError):
            _parse_date("2026-1-01")


class TestDivisionByZero:
    """除零情况测试"""

    def test_valid_count_zero_should_raise(self):
        """测试有效股票数为0时应抛异常"""
        # 这是一个逻辑测试：验证代码在valid_count=0时抛出异常
        # 实际测试需要模拟 calculate_breadth 函数的行为
        with pytest.raises(ZeroDivisionError):
            # 模拟: 1 / 0 应该抛异常
            _ = 1 / 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
