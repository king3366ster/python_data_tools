#!/usr/bin/python2.7 
# -*- coding: utf-8 -*-
import os, time, datetime, re
from DataBuildModel import DataBuildModel
from DataFilterModel import DataFilterModel
from DataStatisticModel import DataStatisticModel
from DataMergeModel import DataMergeModel

class DataDealModel:
    def __init__(self, path = '', needPrint = False, needInterFile = True, logPath = False):
        print 'Date: ', time.strftime("%b %d %Y %H:%M:%S", time.localtime())
        self.path = path
        self.dataMap = dict()
        self.needPrint = needPrint
        self.needInterFile = needInterFile
        if isinstance(logPath, bool):
            if logPath:
                self.logPath = self.path
            else:
                self.logPath = ''
        elif isinstance(logPath, str) or isinstance(logPath, unicode):
            self.logPath = logPath
        else:
            self.logPath = ''

    def formatQueryDate(self, timestamp = ''):
        if isinstance(timestamp, int) or isinstance(timestamp, float):
            ltime = time.localtime(timestamp)
            year = ltime.tm_year
            mon = ltime.tm_mon
            day = ltime.tm_mday
            hour = ltime.tm_hour
            minute = ltime.tm_min
            sec = ltime.tm_sec
            return str(year) + '-' + str(mon) + '-' + str(day) + ' ' + str(hour) + ':' + str(minute) + ':' + str(sec)
        elif isinstance(timestamp, str):
            if re.search(r'\d+\-\d+\-\d+\s+\d+\:\d+\:\d+(\.\d+)?', str(timestamp)): # 时间变量
                return timestamp
            elif re.search(r'\d+\-\d+\-\d+', str(timestamp)):
                return timestamp + ' 0:0:0'
            elif timestamp == 'today':
                now_time = datetime.datetime.now()
                yes_time = yes_time.strftime('%Y-%m-%d 0:0:0')
                return yes_time
            elif timestamp == 'yesterday':
                now_time = datetime.datetime.now()
                yes_time = now_time + datetime.timedelta(days = -1)
                yes_time = yes_time.strftime('%Y-%m-%d 0:0:0')
                return yes_time
        elif isinstance(timestamp, list):
            if len(timestamp) == 6:
                return str(timestamp[0]) + '-' + str(timestamp[1]) + '-' + str(timestamp[2]) + ' ' + str(timestamp[3]) + ':' + str(timestamp[4]) + ':' + str(timestamp[5])
            elif len(timestamp) == 3:
                return str(timestamp[0]) + '-' + str(timestamp[1]) + '-' + str(timestamp[2]) + ' 0:0:0'
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))

    def formatDeltaDate(self, timestr = '', delta_day = -1):
        ltime = time.mktime(time.strptime(timestr, '%Y-%m-%d %H:%M:%S'))
        ltime = time.localtime(ltime + 86400 * delta_day)
        return time.strftime('%Y-%m-%d 0:0:0', ltime)

    def generateSqlCase(self, case_map, field, field_as = None):
        if field_as is None:
            field_as = field
        tmpList = []
        for key in case_map:
            tmpList.append('WHEN `' + field + '` = \"' + key + '\" THEN \"' + case_map[key] + '\"')
        case_str = ' CASE ' + ' '.join(tmpList) + ' END as ' + field_as
        return case_str

    # 数据准备
    def runPrepare(self, prepareData = []):
        # 初始化数据节点
        for dataNode in prepareData:
            dataBuild = DataBuildModel(self.path, logPath = self.logPath)
            
            if 'from_type' in dataNode:
                from_type = dataNode['from_type']
            else:
                if 'sql_db' in dataNode:
                    from_type = 'mysql'
                else:
                    from_type = 'excel'
            target = dataNode['target']
            print 'init target:', target, ' type:', from_type
            if from_type == 'mysql':
                query = dataNode['query']
                if 'use_SQLAlchemy' in dataNode:
                    use_SQLAlchemy = dataNode['use_SQLAlchemy']
                else:
                    use_SQLAlchemy = True
                dataBuild.mysqlConnect(dataNode['sql_db'], use_SQLAlchemy = use_SQLAlchemy)
                df = dataBuild.mysqlQuery(query)
                dataBuild.mysqlDestroy()
                if self.needPrint:
                    print df
                if self.needInterFile:
                    # dataBuild.csvWriter(df, target)
                    dataBuild.excelWriterRaw(df, target)
                self.dataMap[target] = df
            elif from_type == 'csv':
                df = dataBuild.csvReader(target)
                self.dataMap[target] = df
            elif from_type == 'excel':
                if 'sheet_name' in dataNode:
                    sheetName = dataNode['sheet_name']
                else:
                    sheetName = 'Sheet1'
                df = dataBuild.excelReader(target, sheetName)
                self.dataMap[target] = df
            print 'target node:', target, ' saved\n'
        return self.dataMap

    def runCompile(self, formatData = []):
        # 数据节点组合、过滤、统计
        for dataNode in formatData:
            dataBuild = DataBuildModel(self.path, logPath = self.logPath)
            dataFilter = DataFilterModel()
            dataStatistic = DataStatisticModel()
            dataMerge = DataMergeModel()

            target = dataNode['target']     # 目标节点
            srcList = dataNode['src']       # 源节点

            print '//== compile target:', target
            if isinstance(srcList, list) and len(srcList) >= 1:
                srcIndex = 0
                srcDfList = []      # 将中间DataFrame存入列表
                for srcNode in srcList:
                    if isinstance(srcNode, list):
                        srcName = srcNode[0]
                    else:
                        srcName = srcNode

                    # 获取节点矩阵
                    if srcName in self.dataMap:
                        df = self.dataMap[srcName]
                    else:
                        df = dataBuild.excelReader(srcName)
                    print '** deal with src -', srcIndex, ': from ',srcName

                    # 判断是否需要数据统计或数据过滤
                    if isinstance(srcNode, list) and len(srcNode) > 1:       # 有数据统计/过滤相关操作
                        srcDeal = srcNode[1]
                        srcVar = dict()
                        # 数据统计
                        if'statistic' in  srcDeal:
                            print '== start statistic ', srcIndex, ' ==' 
                            for method in srcDeal['statistic']:
                                if method == '':
                                    continue
                                result = dataStatistic.runStatistic(df, method)
                                srcVar[method] = result
                                print '!! ', method, ' - result: ', srcVar[method]
                        # 过滤器
                        if'filter' in  srcDeal:
                            print '== start filter ', srcIndex, ' ==' 
                            for method in srcDeal['filter']:
                                if method == '':
                                    continue
                                df = dataFilter.runFilter(df, method, srcVar)
                                try:
                                    print method, ' : succeed'
                                except:
                                    print 'method succeed'
                                if self.needPrint:
                                    print '!! Temp DataFrame Src', srcIndex
                                    print df

                    # 数据统计/过滤完成
                    srcIndex = srcIndex + 1
                    srcDfList.append(df.copy(deep = True))

                # 合并节点
                if 'method' in dataNode:
                    mergeMethod = dataNode['method']
                else:
                    mergeMethod = dict()
                if len(srcDfList) > 1:
                    print '** merge src list', mergeMethod
                    df = srcDfList[0]
                    for i in range(1, len(srcDfList)):
                        dfTmp = srcDfList[i]
                        df = dataMerge.runMerge(df, dfTmp, mergeMethod)
                    if self.needPrint:
                        print 'Temp DataFrame:'
                        print df
                if self.needInterFile:   
                    try: 
                        dataBuild.excelWriter(df, target)
                    except Exception, what:
                        print 'write direct err: ', what
                        dataBuild.excelWriterRaw(df, target)
                    print 'saved target: ', target                      
                self.dataMap[target] = df
            print '//== compile end\n'
        return self.dataMap
    
    def runSave(self, saveData = []):
        for dataNode in saveData:
            dataBuild = DataBuildModel(self.path, logPath = self.logPath)

            target = dataNode['target']     # 目标节点
            srcName = dataNode['src']       # 源节点
            print '//== save target:', target, '; source:', srcName   

            if srcName in self.dataMap:
                df = self.dataMap[srcName]
            else:
                df = dataBuild.excelReader(srcName)
            
            if 'sql_db' in dataNode:
                from_type = 'mysql'
            else:
                from_type = 'excel'

            if 'if_exists' in dataNode:
                if_exists = dataNode['if_exists']
            else:
                if_exists = 'fail'

            if from_type == 'excel':
                if os.path.exists(self.path + target + '.xlsx'):
                    if if_exists == 'fail':
                        print self.path, target, '.xlsx already exists!'
                    elif if_exists == 'append' or if_exists == 'append_ignore':
                        dataBuild.excelWriterRaw(df, target, if_exists = if_exists)
                        print self.path, target, '.xlsx has been appended'
                    else: # replace
                        try: 
                            dataBuild.excelWriter(df, target)
                        except Exception, what:
                            print 'write direct err: ', what
                            dataBuild.excelWriterRaw(df, target)
                        print self.path, target, '.xlsx has been replaced'
                else:
                    try: 
                        dataBuild.excelWriter(df, target)
                    except Exception, what:
                        print 'write direct err: ', what
                        dataBuild.excelWriterRaw(df, target)
                    print self.path, target, '.xlsx has been saved'

            elif from_type == 'mysql':
                sql_db = dataNode['sql_db']
                unique_key = None
                if 'unique_key' in dataNode:
                    unique_key = dataNode['unique_key']
                if 'need_datetime' in dataNode:
                    need_datetime = dataNode['need_datetime']
                else:
                    need_datetime = True
                    # df = df.set_index(unique_key)
                # dataBuild.mysqlConnect(sql_db)
                dataBuild.mysqlWriter(df, target, sql_db = sql_db, if_exists = if_exists, unique_key = unique_key, need_datetime = need_datetime)
                print 'target saved to mysql: ', target, '; type is ', if_exists
