#!/usr/bin/python2.7 
# -*- coding: utf-8 -*-

import os, sys, time
import json

if __name__ == "__main__":
    value1 = sys.stdin.readline()
    value2 = sys.stdin.readline()
    for i in range(0, 2):
        sys.stdout.write('std %s,%d' % (unicode(value1),i))
        sys.stdout.write(' %s,%d end' % (unicode(value2),i))
        time.sleep(1)
    sys.exit(0)
