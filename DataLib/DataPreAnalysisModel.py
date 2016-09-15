#!/usr/bin/python
# -*- coding: UTF-8 -*-

import pandas as pd
import numpy as np

class DataPreAnalysisModel:
    def __init__(self):
        pass

    # 移动均线相关
    def add_moving_line(self, df, keys, window = 5, suffix = None, type = 'mean'):
        if suffix is not None:
            key_suff = suffix
        else:
            key_suff = type
        real_keys = []
        rename_dict = {}
        for key in keys:
            if key in df.columns:
                real_keys.append(key)
                rename_dict[key] = '%s_%s' % (key, key_suff)
        roll_obj = df.copy().loc[:, real_keys]

        roll_obj = roll_obj.rolling(window = window)
        if type == 'mean':
            roll_obj = roll_obj.mean()
        roll_obj.rename(columns = rename_dict, inplace = True)
        return pd.concat([df, roll_obj], axis = 1)

if __name__ == '__main__':
    df = pd.DataFrame(np.random.randn(6,4), columns=['A', 'B', 'C', 'D'])
    print df
    # rl = df.rolling(window = 3)
    # rm = rl.mean()
    # df = pd.concat([df, rm.loc[:,['A']]], axis=1)
    # print df
    # # print rl.mean()
    t = DataPreAnalysisModel()
    print t.add_moving_line(df, ['A'], window = 4)
