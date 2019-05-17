# -*- coding: utf-8 -*-
import six
import datetime

from quantdigger.engine.series import SeriesBase
from .data_context import DataContext
from .strategy_context import (
    PlotterDelegator,
    TradingDelegator,
    StrategyContext
)
from quantdigger.datastruct import PContract
from quantdigger.engine.series import DateTimeSeries


class Context(PlotterDelegator, TradingDelegator):
    """ 上下文"""
    def __init__(self, data: "dict((s_pcon, DataFrame))",
                 name, settings, strategy, max_window):
        TradingDelegator.__init__(self, name, settings)
        PlotterDelegator.__init__(self)
        self.ctx_dt_series = DateTimeSeries(
            [datetime.datetime(2100, 1, 1)] * max_window,
            'universal_time')
        self.ctx_datetime = datetime.datetime(2100, 1, 1)
        self.ctx_curbar = 0  # update by ExecuteUnit

        self.on_bar = False  # pass to on_bar function or on_symbol function
        self.strategy = strategy
        self._cur_strategy_context = StrategyContext(strategy.name)
        self._cur_data_context = None
        self._data_contexts = {}       # str(PContract): DataContext
        self._strategy_contexts = {}  # TODO
        # latest price data
        self._ticks = {}  # Contract -> float
        self._bars = {}   # Contract -> Bar
        self._init_data_context(data)

    def _init_data_context(self, data: "Dict((strpcon, DataFrame))"):
        for key, raw_data in six.iteritems(data):
            data_context = DataContext(PContract.from_string(key), raw_data)
            self._data_contexts[key] = data_context
            # PContract -- 'IF000.SHEF-10.Minutes'
            # 简化策略用户的合约输入。
            symbol_exchange = key.split('-')[0]
            num_same_contract = filter(
                lambda x: x.startswith(symbol_exchange), data.keys())
            if num_same_contract == 1:
                self._data_contexts[symbol_exchange] = data_context

            symbol = key.split('.')[0]
            num_same_contract = filter(
                lambda x: x.startswith(symbol), data.keys())
            if num_same_contract == 1:
                self._data_contexts[symbol] = data_context

    def switch_to_pcontract(self, s_pcontract):
        self._cur_data_context = self._data_contexts[s_pcontract]

    def update_strategies_env(self, s_pcontract):
        self._cur_strategy_context.set_cur_pcontract(s_pcontract)

    def pcontract_time_aligned(self):
        return (self._cur_data_context.datetime[0] <= self.ctx_datetime and
                self._cur_data_context.next_datetime <= self.ctx_datetime)
        # 第一根是必须运行
        # return  (self._cur_data_context.datetime[0] <= self.ctx_dt_series and
                # self._cur_data_context.ctx_dt_series <= self.ctx_dt_series) or \
                # self._cur_data_context.curbar == 0

    def rolling_forward(self):
        """ 更新最新tick价格，最新bar价格, 环境时间。 """
        def update_context_datetime():
            self.ctx_dt_series.curbar = self.ctx_curbar
            self.ctx_datetime = min(self._cur_data_context.next_datetime,
                                    self.ctx_datetime)
            try:
                self.ctx_dt_series.data[self.ctx_curbar] = self.ctx_datetime
            except IndexError:
                self.ctx_dt_series.data.append(self.ctx_datetime)

        if self._cur_data_context.has_pending_data:
            update_context_datetime()
            return True
        hasnext, data = self._cur_data_context.rolling_forward()
        if not hasnext:
            return False
        update_context_datetime()
        return True

    def update_user_vars(self):
        """ 更新用户在策略中定义的变量, 如指标等。 """
        self._cur_strategy_context.update_user_vars(
            self._cur_data_context._curbar)

    def update_system_vars(self):
        """ 更新用户在策略中定义的变量, 如指标等。 """
        self._cur_data_context.update_system_vars()
        self._ticks[self._cur_data_context.contract] = \
            self._cur_data_context.close[0]
        self._bars[self._cur_data_context.contract] = \
            self._cur_data_context.bar
        oldbar = self._bars.setdefault(
            self._cur_data_context.contract, self._cur_data_context.bar)
        if self._cur_data_context.bar.datetime > oldbar.datetime:
            # 处理不同周期时间滞后
            self._bars[self._cur_data_context.contract] = \
                self._cur_data_context.bar

    def process_trading_events(self, at_baropen):
        super().update_environment(
            self.ctx_datetime, self._ticks, self._bars)
        super().process_trading_events(at_baropen)

    def __getitem__(self, strpcon):
        """ 获取跨品种合约 """
        return self._data_contexts[strpcon.upper()]

    def __getattr__(self, name):
        if hasattr(self._cur_data_context, name):
            return getattr(self._cur_data_context, name)
        elif hasattr(self._cur_strategy_context, name):
            return getattr(self._cur_strategy_context, name)
        else:
            return self.__getattribute__(name)

    def __setattr__(self, name, value):
        if name in [
                '_data_contexts', '_cur_data_context', '_cur_strategy_context',
                'ctx_dt_series', '_ticks', '_bars', 'strategy',
                '_trading', 'on_bar', 'ctx_curbar', 'ctx_datetime',
                'marks', 'blotter', 'exchange', '_orders', '_datetime',
                '_cancel_now', 'events_pool', '_strategy_contexts'
        ]:
            super(Context, self).__setattr__(name, value)
        else:
            #  TODO: check none reserved attribute #
            if isinstance(value, SeriesBase):
                value.reset_data([], self._cur_data_context.size)
            self._cur_strategy_context.add_item(name, value)

    @property
    def strategy_name(self):
        """ 当前策略名 """
        return self._cur_strategy_context.name

    @property
    def pcontract(self):
        """ 当前周期合约 """
        return self._cur_data_context.pcontract

    @property
    def symbol(self):
        """ 当前合约 """
        return str(self._cur_data_context.pcontract.contract)

    @property
    def curbar(self):
        """ 当前是第几根k线, 从1开始 """
        if self.on_bar:
            return self.ctx_curbar + 1
        else:
            return self._cur_data_context.curbar

    @property
    def open(self):
        """ k线开盘价序列 """
        return self._cur_data_context.open

    @property
    def close(self):
        """ k线收盘价序列 """
        return self._cur_data_context.close

    @property
    def high(self):
        """ k线最高价序列 """
        return self._cur_data_context.high

    @property
    def low(self):
        """ k线最低价序列 """
        return self._cur_data_context.low

    @property
    def volume(self):
        """ k线成交量序列 """
        return self._cur_data_context.volume

    @property
    def datetime(self):
        """ k线时间序列 """
        if self.on_bar:
            return self.ctx_dt_series
        else:
            return self._cur_data_context.datetime

