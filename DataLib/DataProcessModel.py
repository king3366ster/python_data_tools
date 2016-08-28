#!/usr/bin/python2.7 
# -*- coding: utf-8 -*-  

import multiprocessing
import subprocess, sys
import time, json, random
from DataThreadModel import DataThreadModel

 
class DataSubProcessModel:
    def __init__(self, file_name, process_num = 1, process_params = []):
        self.file_name = file_name
        self.process_num = process_num
        self.process_params = process_params
        self.process_list = []

    def fork(self, *args, **kwargs):
        proc = subprocess.Popen(['python', self.file_name], stdin = subprocess.PIPE, stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell = False)
        self.process_list.append(proc)
        params = []
        for param in args:
            params.append(param)
        for param in params:
            if isinstance(params, str) or isinstance(params, unicode):
                proc.stdin.write(param)
            else:
                proc.stdin.write(json.dumps(param))
        
    def run(self, isDaemon = True, isJoin = True, timeout = 0):
        self.handle = DataThreadModel(self.fork, self.process_num, self.process_params)
        self.handle.run(isDaemon, isJoin)
        if timeout > 0:
            time.sleep(timeout)
            self.stop()
        return self.getComunication()

    def getComunication(self):
        communications = []
        for proc in self.process_list:
            proc_out, proc_err = proc.communicate()
            communications.append({
                    "pid": proc.pid,
                    "process": proc,
                    "stdout": proc_out,
                    "stderr": proc_err
                })
        return communications

    def stop(self):
        self.handle.stop()
        for proc in self.process_list:
            if proc.poll() is None:
                # proc.terminate()
                proc.kill()
        print 'process all stoped!'


class DataMulProcessModel:
    def __init__(self, fn_name, process_num = 1, process_params = []):
        self.fn_name = fn_name
        self.process_num = process_num
        self.process_params = process_params
        self.process_list = []  
        self.name_pre = 'process_%03d_' % int(random.random() * 1000)   

    # isDaemon 是否是守护进程， isJoin 是否时序并行
    # isJoin True => 串行，False => 并行
    def run(self, isDaemon = False, isJoin = False): 
        for i in range(0, self.process_num):
            name = '%s%03d' % (self.name_pre, i)
            if len(self.process_params) > i:
                param = self.process_params[i]
                if isinstance(param, list):
                    args = tuple(param)
                elif isinstance(param, tuple):
                    args = param
                else:
                    args = (param,)
            else:
                args = ()
            proc = multiprocessing.Process(name = name, target = self.fn_name, args = args)
            proc.daemon = isDaemon # # 则主线程结束时，会把子线程B也杀死，与C/C++中得默认效果是一样的。
            self.process_list.append(proc)
            proc.start()
            if isJoin:
                proc.join()
        print 'processs run'

    def stop(self):
        for proc in self.process_list:
            if proc.is_alive():
                proc.terminate()
                print ('pid %d stoped' % proc.pid)
                proc.join()
        print ('process stoped')

def test_process(num):
    print 'Process:', num
    name = multiprocessing.current_process().name
    print 'Starting:', name
    # sys.exit(0)
    print multiprocessing.current_process().pid

    time.sleep(2)

    print 'Exiting :', name

if __name__ == '__main__':  

    t = DataSubProcessModel('test.py', process_num = 3, process_params = [(2,91),(4,83),(6,)])
    print t.run(timeout = 15)

    # t = DataMulProcessModel(test_process, process_num = 3, process_params = [(6,),(4,),(5,)])
    # t.run()
    # time.sleep(1)
    # t.stop()
