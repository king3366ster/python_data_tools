#!/usr/bin/python2.7 
# -*- coding: utf-8 -*-
from DataLib.DataBuildModel import DataBuildModel
from DataLib.DataDealModel import DataDealModel


# 所需数据库配置
test_db1 = dict({
        'host': '127.0.0.1',
        'user': 'users',
        'pwd': '123456',
        'db': 'test',
        'port': 3306
    })

test_db2 = dict({
        'host': '127.0.0.1',
        'user': 'users',
        'pwd': '123456',
        'db': 'dev',
        'port': 3306
    })

# 所需目标文件源，支持csv/mysql
prepareData = [ 
        # mysql 源数据
        {   'target': 'target_data',
            'sql_db': test_db1,
            'query': 'select * from test_table where id < 100' },
    ]

formatData = [
    ]


saveData = [
        # 存到mysql库中
        {   'target': 'target_table',   
            'src': 'target_data',
            'sql_db': test_db2,
            'unique_key': ['id', 'cid'],
            'if_exists': 'append'
        },
        # 存到data/target_excel.xlsx中
        {   'target': 'target_excel',
            'src': 'target_data',
            'if_exists': 'replace'
        },
    ]

pDeal = DataDealModel('data/', needPrint = False, needInterFile = False, logPath = 'debug')
if locals().has_key('prepareData'):
    dataFrame = pDeal.runPrepare(prepareData)
if locals().has_key('formatData'):
    dataFrame = pDeal.runCompile(formatData)
if locals().has_key('saveData'):
    pDeal.runSave(saveData)


