#!/usr/bin/python2.7 
# -*- coding: utf-8 -*-  

import logging

def initLogger(logPath = ''):
    logger = logging.getLogger() 

    # 文件输出
    if logPath == '':
        logPath = 'debug'
    fh = logging.FileHandler(logPath + '.log')  
    # 再创建一个handler，用于输出到控制台    
    ch = logging.StreamHandler()  
  
    # 定义handler的输出格式formatter    
    formatter = logging.Formatter('%(asctime)s |-| %(levelname)s |-| %(module)s - %(funcName)s - %(lineno)d |-| %(message)s')  
    fh.setFormatter(formatter)  
    ch.setFormatter(formatter)  
  
    #定义一个filter  
    #filter = logging.Filter('mylogger.child1.child2')  
    #fh.addFilter(filter)    
  
    # 给logger添加handler    
    #logger.addFilter(filter)  
    logger.addHandler(fh)  
    logger.addHandler(ch)  

    logger.setLevel(logging.DEBUG)
    
    return logger

# logger = initLogger()
# logger.debug('logger debug message')  
# logger.info('logger info message')  
# logger.warning('logger warning message')  
# logger.error('logger error message')  
# logger.critical('logger critical message')  