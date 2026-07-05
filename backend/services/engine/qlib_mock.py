"""
Qlib Mock Backend
=================
当真实 pyqlib 不可用时（如 ARM 平台缺少预编译 wheel），
提供最小化 mock 实现，保证服务可启动。

注意：mock 模式下回测返回空结果，需安装真实 Qlib + 数据包后才能进行真实回测。
"""

import logging
import pandas as pd

logger = logging.getLogger(__name__)


class _MockData:
    """模拟 qlib.data.D 数据接口"""

    def features(
        self,
        instruments,
        fields,
        start_time=None,
        end_time=None,
        freq="day",
        disk_cache=1,
        **kwargs,
    ):
        logger.debug("MockD.features: returning empty DataFrame")
        return pd.DataFrame()

    def instruments(
        self, market="all", filter_pipe=None, start_time=None, end_time=None, **kwargs
    ):
        logger.debug("MockD.instruments: returning empty list")
        return []

    def calendar(self, start_time=None, end_time=None, freq="day", **kwargs):
        return []

    def expression(self, *args, **kwargs):
        return None

    def uri(self, *args, **kwargs):
        return ""

    def list_instruments(self, *args, **kwargs):
        return {}


D = _MockData()


class _MockBacktestResult:
    """模拟 qlib 回测结果"""

    def __init__(self):
        self.report_normal = pd.DataFrame()
        self.portfolio_normal = pd.DataFrame()
        self.indicator_normal = pd.DataFrame()
        self.positions = pd.DataFrame()
        self.portfolio_analysis = pd.DataFrame()


def backtest(config, *args, **kwargs):
    """Mock backtest function. 返回空结果结构，匹配 qlib.backtest 输出格式。"""
    logger.warning("Mock backtest called - returning empty result (Qlib not available)")
    return _MockBacktestResult()


def init(**kwargs):
    logger.warning("Mock qlib.init called - no-op")
    return True


class _MockQlibModule:
    """模拟 qlib 模块对象"""

    __version__ = "0.0.0-mock"
    init = staticmethod(init)
    backtest = staticmethod(backtest)

    class data:
        D = D

    class config:
        pass
