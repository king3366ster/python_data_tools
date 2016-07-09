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
        # excel 源数据
        {   'target': 'target_excel',
            'sheet_name': 'Sheet1'  },
        # csv 源数据
        {   'target': 'target_csv',
            'from_type': 'csv'  }
    ]

formatData = [

    ]


saveData = [

    ]

# needInterFile = True会自动生成中间excel文件节点，False则数据全在内存中
pDeal = DataDealModel('nim/', needPrint = False, needInterFile = True)
if locals().has_key('prepareData'):
    pDeal.runPrepare(prepareData)
if locals().has_key('formatData'):
    pDeal.runCompile(formatData)
if locals().has_key('saveData'):
    pDeal.runSave(saveData)


