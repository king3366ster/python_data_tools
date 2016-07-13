#!/usr/bin/python2.7 
# -*- coding: utf-8 -*-

import os, re, time, datetime
import math, random
import numpy as np
import pandas as pd

class DataMergeModel:
    def __init__(self, path = ''):
        self.path = path

    def runMerge(self, df1, df2, options = dict()):
        if 'type' in options:
            method = options['type']
        else:
            method = 'merge'

        if 'how' in options:
            how = options['how']
        else:
            if method == 'merge':
                how = 'inner'
            elif method == 'concat':
                how = 'outer'
            else:
                how = 'inner'

        if 'ignore_index' in options:
            ignore_index = options['ignore_index']
        else:
            ignore_index = True

        if 'axis' in options:
            axis = options['axis']
        else:
            axis = 0

        onList = []
        if 'relation' in options:
            relations = options['relation']
            for item in relations:
                if item.find('==') >= 0:
                    items = item.split('==')
                    nameA = items[0]
                    nameB = items[1]

                    # 重命名df2保持命名空间一致
                    df2.rename(columns={nameB: nameA}, inplace=True)
                    onList.append(nameA)
                else:
                    onList.append(item)
        if method == 'merge':
            # print '== start merge: base column:', onList
            if len(onList) > 0:
                return pd.merge(df1, df2, how = how, on = onList)
            else:
                return pd.merge(df1, df2, how = how)
        elif method == 'concat':
            return pd.concat([df1, df2], join = how, axis = axis, ignore_index = True)

    
##    merge(left, right, how='inner', on=None, left_on=None, right_on=None,
##      left_index=False, right_index=False, sort=True,
##      suffixes=('_x', '_y'), copy=True, indicator=False)

##        pd.concat(objs, axis=0, join='outer', join_axes=None, ignore_index=False,
##       keys=None, levels=None, names=None, verify_integrity=False)
