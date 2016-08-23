#!/usr/bin/python2.7 
# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd
import tushare as ts
from DataTimeModel import DataTimeModel

dataTime = DataTimeModel()

class DataTushareModel:
    def __init__(self):
        pass

    def format_date_index(self, df):
        count = df.count()[0]
        id_series = np.arange(count, dtype=int)

        df.insert(0, 'date', pd.to_datetime(df.index))
##        df.rename(columns={'dtime': 'date'}, inplace=True)
        df_new = pd.DataFrame(data=df.values, index=id_series, columns=df.columns.tolist())
        for col in df_new.columns: # 保持类型
            df_new[col] = df_new[col].astype(df[col].dtype)
        return df_new

    def format_date_to_datetime(self, df, t_date = None):
        if t_date is None:
            t_date = dataTime.datetimeRelative(delta = 0)
            t_date = t_date.replace(' 00:00:00', '')
        df_new = df.copy()
        df_new.insert(0, 'datetime', t_date)
        df_new['datetime'] = pd.to_datetime(df_new['datetime'])
        df_new['time'] = pd.to_timedelta(df_new['time'])
        df_new['datetime'] = df_new['datetime'] + df_new['time']
        df_new = df_new.sort_values(['datetime'], ascending=[True])
        del df_new['time']
        
        return df_new
# 历史行情
    # 参数说明：
        # code：股票代码，即6位数字代码，或者指数代码（sh=上证指数 sz=深圳成指 hs300=沪深300指数 sz50=上证50 zxb=中小板 cyb=创业板）
        # start：开始日期，格式YYYY-MM-DD
        # end：结束日期，格式YYYY-MM-DD
        # ktype：数据类型，D=日k线 W=周 M=月 5=5分钟 15=15分钟 30=30分钟 60=60分钟，默认为D
        # retry_count：当网络异常后重试次数，默认为3
        # pause:重试时停顿秒数，默认为0

    # 返回值说明：
        # date：日期
        # open：开盘价
        # high：最高价
        # close：收盘价
        # low：最低价
        # volume：成交量
        # price_change：价格变动
        # p_change：涨跌幅
        # ma5：5日均价
        # ma10：10日均价
        # ma20:20日均价
        # v_ma5:5日均量
        # v_ma10:10日均量
        # v_ma20:20日均量
        # turnover:换手率[注：指数无此项]

    def get_hist_data(self, code, start = None, end = None, ktype='D'):
        if end is None:
            end = dataTime.datetimeRelative(delta = 1)
        if start is None:
            start = dataTime.datetimeRelative(base = end, delta = -20)
        df = ts.get_hist_data(code, start, end, ktype)
        df = self.format_date_index(df)
        return df

# 复权数据
    # 参数说明：
        # code:string,股票代码 e.g. 600848
        # start:string,开始日期 format：YYYY-MM-DD 为空时取当前日期
        # end:string,结束日期 format：YYYY-MM-DD 为空时取去年今日
        # autype:string,复权类型，qfq-前复权 hfq-后复权 None-不复权，默认为qfq
        # index:Boolean，是否是大盘指数，默认为False
        # retry_count : int, 默认3,如遇网络等问题重复执行的次数
        # pause : int, 默认 0,重复请求数据过程中暂停的秒数，防止请求间隔时间太短出现的问题
        
    # 返回值说明：
        # date : 交易日期 (index)
        # open : 开盘价
        # high : 最高价
        # close : 收盘价
        # low : 最低价
        # volume : 成交量
        # amount : 成交金额

    def get_h_data(self, code, start = None, end = None, autype = None, index = False):
        if end is None:
            end = dataTime.datetimeRelative(delta = 1)
        if start is None:
            start = dataTime.datetimeRelative(base = end, delta = -20)
        df = ts.get_h_data(code, start, end, autype = autype, index = index)
        df = self.format_date_index(df)
        return df

# 历史分笔
    # 参数说明：
        # code：股票代码，即6位数字代码
        # date：日期，格式YYYY-MM-DD
        # retry_count : int, 默认3,如遇网络等问题重复执行的次数
        # pause : int, 默认 0,重复请求数据过程中暂停的秒数，防止请求间隔时间太短出现的问题

    # 返回值说明：
        # time：时间
        # price：成交价格
        # change：价格变动
        # volume：成交手
        # amount：成交金额(元)
        # type：买卖类型【买盘、卖盘、中性盘】

    def get_tick_data(self, code, t_date = None):
        if t_date is None:
            t_date = dataTime.datetimeRelative(delta = 0)
        t_date = t_date.replace(' 00:00:00', '')
        df = ts.get_tick_data(code, date = t_date)
        df = self.format_date_to_datetime(df, t_date = t_date)
        return df

if __name__ =='__main__':
    t = DataTushareModel()
##    print t.get_hist_data('600000', start='2015-01-05', end='2015-02-09')
##    print t.get_hist_data('600000')
##    print t.get_h_data('600000')
##    print ts.get_stock_basics()
    # print ts.get_today_all()
##    print t.get_tick_data('600000', t_date='2016-08-23')
    print t.get_tick_data('600000', t_date = '2011-01-06 00:00:00')
    

