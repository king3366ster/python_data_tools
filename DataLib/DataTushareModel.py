#!/usr/bin/python2.7 
# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd
import tushare as ts

class DataTushareModel:
    def __init__(self):
        pass

    def format_date_index(self, df):
        count = df.count()[0]
        id_series = np.arange(count, dtype=int)

        df.insert(0, 'date', pd.to_datetime(df.index))
##        df.rename(columns={'dtime': 'date'}, inplace=True)
        df = pd.DataFrame(data=df.values, index=id_series, columns=df.columns.tolist())
        return df

    # 历史行情
    # code：股票代码，即6位数字代码，或者指数代码（sh=上证指数 sz=深圳成指 hs300=沪深300指数 sz50=上证50 zxb=中小板 cyb=创业板）
    # start：开始日期，格式YYYY-MM-DD
    # end：结束日期，格式YYYY-MM-DD
    # ktype：数据类型，D=日k线 W=周 M=月 5=5分钟 15=15分钟 30=30分钟 60=60分钟，默认为D
    # retry_count：当网络异常后重试次数，默认为3
    # pause:重试时停顿秒数，默认为0
    def get_hist_data(self, code, start='', end='', ktype='D'):
        df = ts.get_hist_data(code, start, end, ktype)
        df = self.format_date_index(df)
        return df

if __name__ =='__main__':
    t = DataTushareModel()
    print t.get_hist_data('600000', start='2015-01-05', end='2015-02-09')

