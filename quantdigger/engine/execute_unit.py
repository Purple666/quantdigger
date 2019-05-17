# -*- coding: utf-8 -*-

import six
from collections import OrderedDict
from datetime import datetime
from quantdigger.config import settings
from quantdigger.datasource.data import DataManager
from quantdigger.engine.context import Context
from quantdigger.engine.profile import Profile
from quantdigger.util import elogger as logger
from quantdigger.util import deprecated
from quantdigger.datastruct import PContract


class ExecuteUnit(object):
    """ 策略执行的物理单元，支持多个组合同时运行。
    """
    def __init__(self,
                 pcontracts,
                 dt_start="1980-1-1",
                 dt_end="2100-1-1",
                 n=None,
                 spec_date={}):  # 'symbol':[,]
        """
        Args:
            pcontracts (list): list of pcontracts(string)

            dt_start (datetime/str): start time of all pcontracts

            dt_end (datetime/str): end time of all pcontracts

            n (int): last n bars

            spec_date (dict): time range for specific pcontracts
        """
        self.finished_data = []
        pcontracts = list(map(lambda x: x.upper(), pcontracts))
        self.pcontracts = pcontracts
        self._combs = []
        self._contexts = []
        self._data_manager = DataManager()
        # str(PContract): DataWrapper
        if settings['source'] == 'csv':
            self.pcontracts = self._parse_pcontracts(self.pcontracts)
        self._default_pcontract = self.pcontracts[0]
        self._all_data, self._max_window = self._load_data(self.pcontracts,
                                                           dt_start,
                                                           dt_end,
                                                           n,
                                                           spec_date)
        self._all_pcontracts = list(self._all_data.keys())

    @deprecated
    def _parse_pcontracts(self, pcontracts):
        # @TODO test
        code2strpcon, exch_period2strpcon = \
            self._data_manager.get_code2strpcon()
        rst = []
        for strpcon in pcontracts:
            strpcon = strpcon.upper()
            code = strpcon.split('.')[0]
            if code == "*":
                if strpcon == "*":  # '*'
                    for key, value in six.iteritems(exch_period2strpcon):
                        rst += value
                else:
                    # "*.xxx"
                    # "*.xxx_period"
                    k = strpcon.split('.')[1]
                    for key, value in six.iteritems(exch_period2strpcon):
                        if '-' in k:
                            if k == key:
                                rst += value
                        elif k == key.split('-')[0]:
                                rst += value
            else:
                try:
                    pcons = code2strpcon[code]
                except IndexError:
                    raise IndexError  # 本地不含该文件
                else:
                    for pcon in pcons:
                        if '-' in strpcon:
                            # "xxx.xxx_xxx.xxx"
                            if strpcon == pcon:
                                rst.append(pcon)
                        elif '.' in strpcon:
                            # "xxx.xxx"
                            if strpcon == pcon.split('-')[0]:
                                rst.append(pcon)
                        elif strpcon == pcon.split('.')[0]:
                            # "xxx"
                            rst.append(pcon)
        return rst

    def add_comb(self, comb: "list(Strategy)", settings):
        """ 添加策略组合组合

        Args:
            comb (list): 一个策略组合
        """
        self._combs.append(comb)
        num_strategy = len(comb)
        if 'capital' not in settings:
            settings['capital'] = 1000000.0  # 默认资金
            logger.info('BackTesting with default capital 1000000.0.')

        assert (settings['capital'] > 0)
        if num_strategy == 1:
            settings['ratio'] = [1]
        elif num_strategy > 1 and 'ratio' not in settings:
            settings['ratio'] = [1.0 / num_strategy] * num_strategy
        assert('ratio' in settings)
        assert(len(settings['ratio']) == num_strategy)
        assert(sum(settings['ratio']) - 1.0 < 0.000001)
        assert(num_strategy >= 1)

        for i, strategy in enumerate(comb):
            iset = {}
            if settings:
                iset = {'capital': settings['capital'] * settings['ratio'][i]}
                self._contexts.append(
                    Context(self._all_data, strategy.name,
                            iset, strategy, self._max_window))
        marks = [ctx.marks for ctx in self._contexts]
        blotters = [ctx.blotter for ctx in self._contexts]
        return Profile(marks, blotters,
                       self._all_data,
                       self.pcontracts[0],
                       len(self._combs) - 1)

    def _init_strategies(self):
        for s_pcontract in self._all_pcontracts:
            for context in self._contexts:
                context.switch_to_pcontract(s_pcontract)
                context.update_strategies_env(s_pcontract)
                context.strategy.on_init(context)

    def run(self):
        logger.info("runing strategies...")
        # 初始化策略自定义时间序列变量
        self._init_strategies()

        has_next = True
        while True:
            # Feeding data of latest.
            toremove = set()
            for s_pcontract in self._all_pcontracts:
                for context in self._contexts:
                    context.switch_to_pcontract(s_pcontract)
                    has_next = context.rolling_forward()
                    if not has_next:
                        toremove.add(s_pcontract)
            if toremove:
                for s_pcontract in toremove:
                    self._all_pcontracts.remove(s_pcontract)
                if len(self._all_pcontracts) == 0:
                    # 策略退出后的处理
                    for context in self._contexts:
                        context.switch_to_pcontract(self._default_pcontract)
                        context.update_strategies_env(self._default_pcontract)
                        # 异步情况下不同策略的结束时间不一样。
                        context.strategy.on_exit(context)
                    return

            # Updating global context variables like
            # close price and context time.
            for s_pcontract in self._all_pcontracts:
                for context in self._contexts:
                    context.switch_to_pcontract(s_pcontract)
                    if context.pcontract_time_aligned():
                        context.update_system_vars()
                        context._cur_data_context.has_pending_data = False
            # Calculating user context variables.
            for s_pcontract in self._all_pcontracts:
                # Iterating over combinations.
                for context in self._contexts:
                    context.switch_to_pcontract(s_pcontract)
                    if not context.pcontract_time_aligned():
                        continue
                    context.update_strategies_env(s_pcontract)
                    context.update_user_vars()
                    context.on_bar = False
                    context.strategy.on_symbol(context)

            # 遍历组合策略每轮数据的最后处理
            tick_test = settings['tick_test']
            for context in self._contexts:
                # 确保单合约回测的默认值
                context.switch_to_pcontract(self._default_pcontract)
                context.update_strategies_env(self._default_pcontract)
                context.on_bar = True
                # 确保交易状态是基于开盘时间的。
                context.process_trading_events(at_baropen=True)
                context.strategy.on_bar(context)
                if not tick_test:
                    # 保证有可能在当根Bar成交
                    context.process_trading_events(at_baropen=False)
                context.ctx_datetime = datetime(2100, 1, 1)
                context.ctx_curbar += 1


    def _load_data(self, strpcons, dt_start, dt_end, n, spec_date):
        all_data = OrderedDict()
        max_window = -1
        logger.info("loading data...")
        pcontracts = [PContract.from_string(s) for s in strpcons]
        pcontracts = sorted(pcontracts, key=PContract.__str__, reverse=True)
        for i, pcon in enumerate(pcontracts):
            strpcon = str(pcon)
            if strpcon in spec_date:
                dt_start = spec_date[strpcon][0]
                dt_end = spec_date[strpcon][1]
            assert(dt_start < dt_end)
            if n:
                raw_data = self._data_manager.get_last_bars(strpcon, n)
            else:
                raw_data = self._data_manager.get_bars(strpcon, dt_start, dt_end)
            if len(raw_data) == 0:
                continue
            all_data[strpcon] = raw_data
            max_window = max(max_window, len(raw_data))

        if n:
            assert(max_window <= n)
        if len(all_data) == 0:
            assert(False)
            # @TODO raise
        return all_data, max_window
