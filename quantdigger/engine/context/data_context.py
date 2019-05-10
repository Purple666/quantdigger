# -*- coding: utf-8 -*-

import datetime

from quantdigger.engine.series import NumberSeries, DateTimeSeries
from quantdigger.util import elogger as logger
from quantdigger.datastruct import Bar


class DataContext(object):
    """ A DataContext expose data should be visited by multiple strategie.
    which including bars of specific PContract.
    """
    def __init__(self, helper):
        data = helper.data
        self.open = NumberSeries(data.open.values, 'open')
        self.close = NumberSeries(data.close.values, 'close')
        self.high = NumberSeries(data.high.values, 'high')
        self.low = NumberSeries(data.low.values, 'low')
        self.volume = NumberSeries(data.volume.values, 'volume')
        self.datetime = DateTimeSeries(data.index, 'datetime')
        self.ith_comb = -1   # 第i个组合
        self.ith_strategy = -1   # 第j个策略
        self.bar = Bar(None, None, None, None, None, None)
        self.new_row = False
        self.next_datetime = datetime.datetime(2100, 1, 1)
        self.size = len(data.close)
        self._curbar = -1
        self._helper = helper

    @property
    def raw_data(self):
        return self._helper.data

    @property
    def curbar(self):
        return self._curbar + 1

    @property
    def pcontract(self):
        return self._helper.pcontract

    @property
    def contract(self):
        return self._helper.pcontract.contract

    def update_system_vars(self):
        # self.data = np.append(data, tracker.container_day)
        self._curbar = self.last_curbar
        self.open.update_curbar(self._curbar)
        self.close.update_curbar(self._curbar)
        self.high.update_curbar(self._curbar)
        self.low.update_curbar(self._curbar)
        self.volume.update_curbar(self._curbar)
        self.datetime.update_curbar(self._curbar)
        self.bar = Bar(self.datetime[0], self.open[0], self.close[0],
                       self.high[0], self.low[0], self.volume[0])
        self.new_row = False

    def rolling_forward(self):
        """ 滚动读取下一步的数据。 """
        self.new_row, self.last_curbar = self._helper.rolling_forward()
        if not self.new_row:
            self.last_curbar -= 1
            return False, None
        self.next_datetime = self._helper.data.index[self.last_curbar]
        if self.datetime[0] >= self.next_datetime and self.curbar != 0:
            logger.error('合约[%s] 数据时间逆序或冗余' % self.pcontract)
            raise
        return True, self.new_row

    def __len__(self):
        return len(self._helper)
