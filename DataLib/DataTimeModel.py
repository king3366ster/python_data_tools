#!/usr/bin/python2.7 
# -*- coding: utf-8 -*-
import time, datetime, re

class DataTimeModel():
    # 如时间戳 => 字符串2016-01-21
    def tranformTimeStamp(self, timeStamp):
        timeArray = time.localtime(timeStamp)
        timeString = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
        return timeString

    # 如字符串2016-01-21 => 时间戳
    def tranformTimeString(self, timeString):
        timeArray = time.strptime(timeString, "%Y-%m-%d %H:%M:%S")
        timeStamp = int(time.mktime(timeArray))
        return timeStamp

    def datetimeToday(self):
        timeArray = time.localtime(time.time())
        timeString = time.strftime("%Y-%m-%d 00:00:00", timeArray)
        return timeString

    def datetimeNow(self):
        timeArray = time.localtime(time.time())
        timeString = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
        return timeString

    def datetimeRelative(self, base = '', delta = 0):
        if base == '':
            basetime = datetime.datetime.now()
        elif re.match('\d+\-\d+\-\d+ \d+:\d+:\d+', unicode(base)):
            timeArray = time.strptime(base, "%Y-%m-%d %H:%M:%S")
            timeStamp = time.mktime(timeArray)
            basetime = datetime.datetime.fromtimestamp(timeStamp)
        elif re.match('\d+\-\d+\-\d+', unicode(base)):
            timeArray = re.split('\-', base)
            timeArray = map(int, timeArray)
            basetime = apply(datetime.datetime, timeArray)
        elif isinstance(base, (long, int, float)):
            basetime = datetime.datetime.fromtimestamp(base)
        deltatime = datetime.timedelta(days = delta)
        timeArray = basetime + deltatime
        timeString = timeArray.strftime("%Y-%m-%d 00:00:00")
        return timeString
    
if __name__ =='__main__':
    tc = DataTimeModel()
    print tc.tranformTimeStamp(1471765449)
    print tc.tranformTimeString('2016-8-13 0:0:0')
    print tc.datetimeToday()
    print tc.datetimeNow()
    print tc.datetimeRelative(base='2016-8-12', delta = -1)
    print tc.datetimeRelative(base='2016-8-12 18:02:23', delta = -1)
    print tc.datetimeRelative(base=1471017600, delta = -1)
    
    
