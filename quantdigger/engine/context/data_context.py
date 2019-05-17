# -*- coding: utf-8 -*-

import datetime

from quantdigger.engine.series import NumberSeries, DateTimeSeries
from quantdigger.util import elogger as logger
from quantdigger.datastruct import Bar


class DataWrapper(object):
    """ 数据源包装器，使相关数据源支持逐步读取操作 """

    def __init__(self, max_length):
        self.curbar = -1
        self._max_length = max_length

    def __len__(self):
        return self._max_length

    def rolling_forward(self):
        """ 读取下一个数据"""
        self.curbar += 1
        if self.curbar == self._max_length:
            self.curbar -= 1
            return False, self.curbar
        else:
            return True, self.curbar


class DataContext(object):
    """ A DataContext expose data should be visited by multiple strategie.
    which including bars of specific PContract.
    """
    def __init__(self, pcontract, raw_data):
        self.open = NumberSeries(raw_data.open.values, 'open')
        self.close = NumberSeries(raw_data.close.values, 'close')
        self.high = NumberSeries(raw_data.high.values, 'high')
        self.low = NumberSeries(raw_data.low.values, 'low')
        self.volume = NumberSeries(raw_data.volume.values, 'volume')
        self.datetime = DateTimeSeries(raw_data.index, 'datetime')
        self.bar = Bar(None, None, None, None, None, None)
        self.has_pending_data = False
        self.next_datetime = datetime.datetime(2100, 1, 1)
        self.size = len(raw_data.close)
        self.pcontract = pcontract
        self._curbar = -1
        self._helper = DataWrapper(len(raw_data))
        self._raw_data = raw_data

    @property
    def raw_data(self):
        return self._helper.data

    @property
    def curbar(self):
        return self._curbar + 1

    @property
    def contract(self):
        return self.pcontract.contract

    def update_system_vars(self):
        # self.data = np.append(data, tracker.container_day)
        self._curbar = self._next_bar
        self.open.update_curbar(self._curbar)
        self.close.update_curbar(self._curbar)
        self.high.update_curbar(self._curbar)
        self.low.update_curbar(self._curbar)
        self.volume.update_curbar(self._curbar)
        self.datetime.update_curbar(self._curbar)
        self.bar = Bar(self.datetime[0], self.open[0], self.close[0],
                       self.high[0], self.low[0], self.volume[0])

    def rolling_forward(self):
        """ 滚动读取下一步的数据。 """
        self.has_pending_data, self._next_bar = self._helper.rolling_forward()
        if not self.has_pending_data:
            return False, None
        self.next_datetime = self._raw_data.index[self._next_bar]
        if self.datetime[0] >= self.next_datetime and self.curbar != 0:
            logger.error('合约[%s] 数据时间逆序或冗余' % self.pcontract)
            raise
        return True, self.has_pending_data

    def __len__(self):
        return len(self._helper)
