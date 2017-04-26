#!/usr/bin/python
#coding:utf8

import os, sys
from datetime import *
from var import *
from log import *

# 条件、结果检查类
class Checker:
    """docstring for DbHelper"""
    def __init__(self, arg):
        self.arg = arg
        
    # GP 流水分区表检查

    # GP 拉连表检查

    # HDFS文件检查

    # HIVE内部表分区检查

    # HIVE外部表分区检查

# 本地文件是否存在检查
def has_local_file(fileName):
    pass

# 本地文件FTP传输完成检查
def finished_local_file(fileName):
    if not os.path.exists(fileName):
        return False

    # 判断文件停止修改30分钟以上
    stat = os.stat(fileName)
    fileModiftDateTime = datetime.fromtimestamp(stat.st_mtime)
    interval = datetime.now() - fileModiftDateTime
    if (interval.days > 0 or interval.seconds > 30*60) and stat.st_size > 0:
        return True
    else:
        return False
    # print type(inteval)
    # fileModifyTime = datetime.strftime('%Y%m%d %H:%M:%S', time.localtime(stat.st_mtime))


# 本地文件可解压检查

# 本地文件可解包检查
    
def main():
    print finished_local_file('/home/pi/ts150/non_self/7_scheduler/hadoop_check_fun.sh')
    print finished_local_file('/home/pi/ts150/non_self/7_scheduler/check.py')
    print finished_local_file('/home/pi/ts150/non_self/7_scheduler/error.log')

if __name__ == '__main__':
    main()