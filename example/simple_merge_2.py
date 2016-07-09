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
        {   'target': 'target_data1',
            'sql_db': test_db1,
            'query': 'select * from db1_table where id < 100' },
        # mysql 源数据
        {   'target': 'target_data2',
            'sql_db': test_db2,
            'query': 'select * from db2_table where id < 100' },
    ]

formatData = [

        {   'target': 'target_data',
            'method': {
                    'type': 'merge',
                    'how': 'inner',         # inner outer left right
                    'relation': ['cid']
                },
            'src': [
                ['target_data1', {'statistic':['cid|count','cid|mean']}],
                ['target_data2',{'filter': ['is_test<1','shape:{ drop :[is_test]}'] }],
            ]
        }
    ]


saveData = [

    ]

pDeal = DataDealModel('data/', needPrint = False, needInterFile = True)
if locals().has_key('prepareData'):
    pDeal.runPrepare(prepareData)
if locals().has_key('formatData'):
    pDeal.runCompile(formatData)
if locals().has_key('saveData'):
    pDeal.runSave(saveData)
