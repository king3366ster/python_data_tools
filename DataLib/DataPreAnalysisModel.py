#!/usr/bin/python
# -*- coding: UTF-8 -*-

import pandas as pd
import numpy as np

class DataPreAnalysisModel:
    def __init__(self):
        pass

    # 移动均线相关
    def add_moving_line(self, df, keys, window = 5, type = 'mean'):
        roll_obj = df.rolling(window = window)
        key_suff = type
        if type == 'mean':
            roll_obj = roll_obj.mean()
        else:
            roll_obj = df.copy()

        real_keys = []
        rename_dict = {}
        for key in keys:
            if key in roll_obj.columns:
                real_keys.append(key)
                rename_dict[key] = '%s_%s' % (key, key_suff)
        roll_obj = roll_obj.loc[:, real_keys]
        roll_obj.rename(columns = rename_dict, inplace = True)

        return pd.concat([df, roll_obj], axis = 1)

if __name__ == '__main__':
    df = pd.DataFrame(np.random.randn(6,4), columns=['A', 'B', 'C', 'D'])
    # rl = df.rolling(window = 3)
    # rm = rl.mean()
    # df = pd.concat([df, rm.loc[:,['A']]], axis=1)
    # print df
    # # print rl.mean()
    t = DataPreAnalysisModel()
    print t.add_moving_line(df, ['A'], window = 4)
