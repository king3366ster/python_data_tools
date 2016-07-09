#!/usr/bin/python2.7 
# -*- coding: utf-8 -*-

import os, re, time, datetime
import math, random
import numpy as np
import pandas as pd


class DataFilterModel:
    def __init__(self, path = ''):
        self.path = path

    def _removeBrace(self, method):
        if re.search(r'\{\s*[^\{\}]+\s*\}', method): # { } 包裹的特殊变量
            varKey = re.findall(r'\{\s*([^\{\}]+)\s*\}', method)
            method = varKey[0]
        return method

    def _removeBraceOuter(self, method):
        if re.search(r'^\s*\{\s*(.+)\s*\}\s*$', method): # 仅判断是否拥有{ }
            varKey = re.findall(r'^\s*\{\s*(.+)\s*\}\s*$', method)
            method = varKey[0]
        return method    

    def _parseStrArray(self, value):
        if not isinstance(value, unicode):
            value = str(value)
        if value.find('[') >= 0:
            value = value.replace('[','').replace(']','')
            value = value.strip()
            temp = re.split('\s*,\s*', value)  
            return temp  
        else:
            return []

    def runFilter(self, df, method, srcVar = dict()):
        method = method.strip() #去除首尾空格

        if method.find(':') >= 0:   # 把key剥离出来
            indexColons = method.find(':')
            if method.find('{') >= 0 and indexColons > method.find('{'):    # 第一个 : 在{ }里的情形，不存在过滤器名
                func = 'operator'
                method = method
            else:
                func = method[ : indexColons].strip()
                method = method[indexColons + 1 : ]
        else:
            func = 'operator'   # 默认方法
            method = method
            
        # 特殊变量的计算组装
        cmpVar = None
        if re.search(r'\{[^\{\}]+\}', method): # { } 包裹的特殊变量 {date|mean}, {2015-9-11 19:01:1}
            varKey = self._removeBraceOuter(method) # 可能存在诸如 timefunc:{date>{2015-9-11}
            varKey = self._removeBrace(varKey)
            if srcVar.has_key(varKey):      # 统计所得变量
                cmpVar = srcVar[varKey]
                # print 'compare variable:',cmpVar
            elif re.search(r'\d+\-\d+\-\d+\s+\d+\:\d+\:\d+', varKey): # 时间变量
                func = 'timefunc'
            elif re.search(r'\d+\-\d+\-\d+', varKey):
                func = 'timefunc'

        if func == 'operator':
            return self.filterOperator(df, method, cmpVar)
        elif func == 'timefunc':
            return self.filterTimefunc(df, method)
        elif func == 'in':
            return self.filterIn(df, method)
        elif func == 'loc':
            return self.filterLoc(df, method)
        elif func == 'iloc':
            return self.filterIloc(df, method)
        elif func == 'sort':
            return self.filterSort(df, method)
        elif func == 'shape':
            return self.filterShape(df, method)
        elif func == 'rename':
            return self.filterRename(df, method)
        elif func == 'operatorCol':
            return self.filterOperatorCol(df, method)
        elif func == 'nan':
            return self.filterNan(df, method)
        elif func == 'rawFunc':
            return self.filterRawFunc(df, method)
        return df
        
    # 数据比较器
    # 例如'A > 2',  ' E<{A|mean} ',  ' C=”colume” ',     'operator:{A >= 2}'
    def filterOperator(self, df, method, cmpVar = None):
        method = self._removeBraceOuter(method)
        temp = re.split('\s*(>|<|=|!)+\s*',method)
        key = temp[0]

        try:
            df_key = df[key]
        except Exception, what:
            print '!! Error: DataFrame doesn\'t has key ', what
            return df
        
        if cmpVar is not None:
            value = cmpVar
        else:
            value = temp[2]
                
        if str(value).find('"') >= 0:    #字符串类型
            value = value.replace('"','')
        else:
            value = float(value)
            
        if method.find('>=') >= 0:
            return df[df[key]>=value]
        elif method.find('<=') >= 0:
            return df[df[key]<=value]
        elif method.find('!=') >= 0:
            return df[df[key]!=value]
        elif method.find('=') >= 0:
            return df[df[key]==value]
        if method.find('>') >= 0:
            return df[df[key]>value]
        elif method.find('<') >= 0:
            return df[df[key]<value]
        return df

    # 'in:{A:[1, 3, 5]}'
    def filterIn(self, df, method):
        method = self._removeBrace(method)
        temp = re.split('\s*\:\s*',method)
        key = temp[0]

        try:
            df_key = df[key]
        except Exception, what:
            print '!! Error: DataFrame doesn\'t has key ', what
            return df
        
        value = temp[1]
        if value.find('[') >= 0:
            temp = self._parseStrArray(value)   # 字符型数组切换真实数组
            inList = []
            for item in temp:
                if item.find('"') >= 0:
                    inList.append(item.replace('"',''))
                elif re.search(r'\d*\.\d+', item):
                    inList.append(float(item))
                elif re.search(r'\d+', item):
                    inList.append(int(item))
            return df[df[key].isin(inList)]
        return df

    def filterLoc(self, df, method):
        method = self._removeBrace(method)
        method = 'df=df.loc['+method+']'
        exec(method)
        return df

    # iloc:{2:6}
    def filterIloc(self, df, method):
        method = self._removeBrace(method)
        method = 'df=df.iloc['+method+']'
        exec(method)
        return df

    def filterIx(self, df, method):
        method = self._removeBrace(method)
        method = 'df=df.ix['+method+']'
        exec(method)
        return df

    # 'sort:{[B, C], [1, 0]}',        'sort:{[A, C]}'
    def filterSort(self, df, method):
        method = self._removeBrace(method)
        params = re.findall(r'\[([^\[\]]+)\]',method)
        param1 = params[0].split(',')
        if len(params) < 2:
            param2 = []
            df = df.sort_values(by = param1)
        else:
            param2 = params[1].split(',')
            param2_int = [];
            for i in range(0,len(param2)):
                param2_int.append(int(param2[i]))
            df = df.sort_values(by = param1, ascending = param2_int)
        return df

    # 'rename:{D->M,B->F}'
    def filterRename(self, df, method):
        method = self._removeBrace(method)
        params = method.split(',')
        for item in params:
            item = item.strip()
            names = re.split('\s*->\s*',item)
            if len(names) == 2:
                nameA = names[1]
                nameB = names[0]
                df.rename(columns={nameB: nameA}, inplace=True)
        return df

    # 'shape:{ drop :[A,B]}', 'shape:{left:[D_y,C,D_x]}', 'shape:{append:[key, value]}'
    def filterShape(self, df, method):
        method = self._removeBrace(method)
        temp = re.split('\s*:\s*', method)
        shapeType = temp[0]
        columns = self._parseStrArray(temp[1])
        if shapeType == 'drop':
            df_new = df.drop(columns, axis=1)
        elif shapeType == 'left':
            # df_new = df.copy(deep = True)
            df_new = df.loc[:,columns]
        elif shapeType == 'append':
            df_new = df.copy(deep = True)
            df_new.insert(len(df.columns), columns[0], columns[1])
        else:
            df_new = df.copy(deep = True)
        return df_new

    # 矩阵列相关四则计算
    # 'operatorCol:{ M = - C * (D_x + 2) - D_x}'
    # 'operatorCol:{ N = (date2 - date1)/3600.0/24.0 }'
    def filterOperatorCol(self, df, method):
        method = self._removeBraceOuter(method)
        colnum = len(df.columns)

        keydest = re.findall('^\s*([\S]+)\s*=', method)
        keydest = keydest[0]

        method = method[method.find('=')+1:]

        # 先对列名、符合进行分词
        tmpChar = ''
        tmpCharType = 0 # 0 \s; 1 +-*/; 2 ()[]{} ;3 char
        tmpCharTypePre = 0
        tmpCharList = []
        for i in range(0, len(method)):
            iChar = method[i]
            if re.match('\s', iChar):
                continue
            if re.match('[\+\-\*\/]', iChar):
                tmpCharType = 1
            elif re.match('[\(\)\[\]]', iChar):
                tmpCharType = 2
            else:
                tmpCharType = 3
            if tmpCharType != tmpCharTypePre:
                tmpCharList.append(tmpChar)
                tmpChar = iChar
            else:
                tmpChar = tmpChar + iChar
            tmpCharTypePre = tmpCharType
        tmpCharList.append(tmpChar)
        if tmpCharList[0] == '':
            del tmpCharList[0]
        # print tmpCharList

        # 建堆栈进行计算
        # stackSymbol = [] # 符号栈
        # stackNumber = [] # 数值栈
        operation = 'tmpCol = '
        tmpCharTypePre = -1 # 优先级 3:() 2:*/ 1:+-
        for tmpChar in tmpCharList:
            if re.match('[\[\]\(\)]', tmpChar):
                tmpCharType = 3
            elif re.match('[\*\/]', tmpChar):
                tmpCharType = 2
            elif re.match('[\+\-]', tmpChar):
                tmpCharType = 1
            else:
                tmpCharType = 0
            if tmpCharType == 0: # keyname

                if re.match('^\d*(\.\d+)*$',tmpChar):
                    operation = operation + tmpChar
                else:
                    if tmpChar.find('"') >= 0:
                        keyname = tmpChar.replace('"')
                    else:
                        keyname = tmpChar
                        
                    if df[keyname].dtype == 'timedelta64[ns]':
                        operation = operation + 'df[\'' + keyname + '\'] / np.timedelta64(1, \'s\')'
                    else:
                        operation = operation + 'df[\'' + keyname + '\']'                
            else:
                operation = operation + tmpChar
        # print operation
        exec(operation)
        if tmpCol.dtype == 'timedelta64[ns]':
            tmpCol = self.transformNpDatetime(tmpCol)
        df.insert(colnum, keydest, tmpCol)
        return df

    def filterTimefunc(self, df, method):
        method = self._removeBraceOuter(method)

        temp = re.split('\s*(>|<|=)+\s*',method)
        key = temp[0]
        value = str(temp[2])

        try:
            df_key = df[key]
        except Exception, what:
            print '!! Error: DataFrame doesn\'t has key ', what
            return df  
            
        if re.search(r'\{[^\{\}]+\}', value):
            varKey = re.findall(r'\{([^\{\}]+)\}', value)
            value = varKey[0]

##      #将所有时间类型转化为时间戳
        value = self.transformDatetime(value)
        ltime = time.localtime(value)
        dtime = datetime.datetime(ltime.tm_year, ltime.tm_mon, ltime.tm_mday, ltime.tm_hour, ltime.tm_min, ltime.tm_sec)
        dtime64 = np.datetime64(dtime)
        # format time
        if df[key].dtype == 'datetime64[ns]':
            pass
        elif isinstance(df[key][0], unicode) or isinstance(df[key][0], str):
            df[key] = pd.to_datetime(df[key])
        else:
            df[key] = pd.to_datetime(df[key] * 1000000000) # 按秒级别计算
        
        if method.find('>=') >= 0:
            return df[df[key] >= dtime64]
        elif method.find('<=') >= 0:
            return df[df[key] <= dtime64]
        elif method.find('=') >= 0:
            return df[df[key] == dtime64]
        if method.find('>') >= 0:
            return df[df[key] > dtime64]
        elif method.find('<') >= 0:
            return df[df[key] < dtime64]
        return df   

    #将所有时间类型转化为时间戳
    def transformDatetime(self, dtime):
        if re.search(r'\d+\-\d+\-\d+\s+\d+\:\d+\:\d+', str(dtime)): # 时间变量
            mtime = time.mktime(time.strptime(dtime,'%Y-%m-%d %H:%M:%S'))
        elif re.search(r'\d+\-\d+\-\d+', str(dtime)):
            mtime = time.mktime(time.strptime(dtime,'%Y-%m-%d'))
        else:
            try:
                dateType = dtime.dtype
                if re.search('time', dtime.dtype):
                    mtime = dtime / np.timedelta64(1, 'ns')
            except Exception, what:
                print what
            mtime = int(dtime)
        return mtime

    def filterNan(self, df, method):
        method = self._removeBraceOuter(method)
        if method.find(':') < 0:
            naType = method
            naValue = ''
        else:
            temp = re.split('\s*:\s*', method)
            naType = temp[0]
            naValue = temp[1]
        if naType == 'drop':
            df_new = df.dropna()
        elif naType == 'fill':
            df_new = df.fillna(naValue)
        else:
            df_new = df.copy(deep = True)
        return df_new


    # rawFunc: { df.xxx[]}
    def filterRawFunc(self, df, method):
        method = self._removeBraceOuter(method)
        method = 'df = ' + method
        exec(method)
        return df
        
