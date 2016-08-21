#!/usr/bin/python2.7 
# -*- coding: utf-8 -*-
import time, datetime

class DataTimeModel():
    # 如字符串2016-01-21 => 时间戳
    def tranformTimeStamp(self, timeStamp):
        timeArray = time.localtime(timeStamp)
        otherStyleTime = time.strftime("%Y-%m-%d", timeArray)
        return otherStyleTime

    # 如时间戳 => 字符串2016-01-21
    def tranformTimeString(self, timeString):
        timeArray = time.strptime(timeString, "%Y-%m-%d %H:%M:%S")
        timeStamp = int(time.mktime(timeArray))
        return timeStamp

if __name__ =='__main__':
    tc = DataTimeModel()
    print tc.tranformTimeStamp(1471765449)
    print tc.tranformTimeString('2016-8-13 0:0:0')
    
