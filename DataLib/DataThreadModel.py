#!/usr/bin/python2.7 
# -*- coding: utf-8 -*-  

from multiprocessing import Process
import os, random
import time
import threading
import inspect, ctypes

class PrivateThread(threading.Thread):
    def __init__(self, target, args = (), name = None):
        super(PrivateThread, self).__init__(target = target, args = args, name = name)

    def _async_raise(self, tid, exctype):
        """raises the exception, performs cleanup if needed"""
        if not inspect.isclass(exctype):
            raise TypeError("Only types can be raised (not instances)")
        res = ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, ctypes.py_object(exctype))
        if res == 0:
            raise ValueError("invalid thread id")
        elif res != 1:
            # """if it returns a number greater than one, you're in trouble, 
            # and you should call it again with exc=NULL to revert the effect"""
            ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, 0)
            raise SystemError("PyThreadState_SetAsyncExc failed")
        
    def _get_my_tid(self):
        """determines this (self's) thread id"""
        if not self.isAlive():
            raise threading.ThreadError("the thread is not active")
        # do we have it cached?
        if hasattr(self, "_thread_id"):
            return self._thread_id
        # no, look for it in the _active dict
        for tid, tobj in threading._active.items():
            if tobj is self:
                self._thread_id = tid
                return tid
        raise AssertionError("could not determine the thread's id")
    
    def raise_exc(self, exctype):
        """raises the given exception type in the context of this thread"""
        self._async_raise(self._get_my_tid(), exctype)
        
    def stop(self):
        """raises SystemExit in the context of the given thread, which should 
            cause the thread to exit silently (unless caught)"""
        self.raise_exc(SystemExit)
 
class DataThreadModel:
    def __init__(self, thread_fn, thread_num = 1, thread_params = []):
        self.name_pre = 'thread_%03d_' % int(random.random() * 1000)
        self.thread_list = []
        for i in range(0, thread_num):
            if len(thread_params) > i:
                args = thread_params[i]
            else:
                args = ()
            tname = '%s%03d' % (self.name_pre, i)
##            thd = threading.Thread(target = thread_fn, args = args, name = tname)
            thd = PrivateThread(target = thread_fn, args = args, name = tname)
            self.thread_list.append(thd)

    # isDaemon 是否是守护进程， isJoin 是否时序并行
    def run(self, isDaemon = False, isJoin = True):
        for thd in self.thread_list:
            if isDaemon: # 则主线程结束时，会把子线程B也杀死，与C/C++中得默认效果是一样的。
                thd.setDaemon(isDaemon)   # 设置守护线程
            thd.start()
        if isJoin:
            for thd in self.thread_list:
                thd.join() # 如果join，主线程在从线程完成之后再继续
##        thd.join()
        print 'threads run'
                
    def stop(self):
        for thd in self.thread_list:
            if thd.isAlive():   
                thd.stop() 
                continue
        print 'threads stoped'
            
    def getActiveCount(self):
        return threading.activeCount() - 1


if __name__ == '__main__':  

    def test(num):
        for i in range(0, 5):
            time.sleep(2 * random.random())
            print num
        
    t = DataThreadModel(test, 3, [(12,),(22,),(32,)])
    t.run(False, False)
    time.sleep(3)
##    t.stop()
    print 'over'
