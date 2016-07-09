#!/usr/bin/python2.7 
# -*- coding: utf-8 -*-

import os, re, time
import numpy as np
import pandas as pd


class DataStatisticModel:
    def __init__(self, path = ''):
        self.path = path

    def runStatistic(self, df, method):
        temp = method.split('|')
        if len(temp) >= 2:
            key = temp[0]
            method = temp[1]
        elif len(temp) < 2:
            key = ''
            method = method
            
        try:
            df_key = df[key]
        except Exception, what:
            print '!! Error: DataFrame doesn\'t has key ', what
            return None

        if method == 'mean':
            return df[key].mean()
        
        elif method == 'count':
            return df[key].count()

        elif method == 'mad':
            return df[key].mad()
        
        elif method == 'median':
            return df[key].median()

        elif method == 'max':
            return df[key].max()

        elif method == 'min':
            return df[key].min()

        elif method == 'var':
            return df[key].var()

        elif method == 'std':
            return df[key].std()

        elif method == 'sum':
            return df[key].sum()

        elif method == 'mode':
            return df[key].mode()
