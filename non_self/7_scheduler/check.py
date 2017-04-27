#!/usr/bin/python
#coding:utf8

import os, sys, re
from datetime import *
from var import *
from log import *
from common_fun import *

# 条件、结果检查类
class Checker:
    """docstring for DbHelper"""
    def __init__(self, arg):
        self.arg = arg
        
    # GP 流水分区表检查

    # GP 拉连表检查

# HDFS文件检查
def has_hdfs_file(path, check_file_size=True):
    pattern = re.compile(r'\s+')
    fileSizeTotal = 0
    fileNumTotal = 0
    processing = False

    cmd = 'hadoop fs -ls -R %s' % path
    for i in range(3):
        retcode, outlines = executeShell(cmd)
        if retcode != 0:
            continue
        for line in outlines:
            # 目录不计入文件
            if len(line) == 0 or line[0] == 'd':
                continue
            # ls内容按列分割
            line_array = re.split(pattern, line)
            fileSize = line_array[4]
            fileDate = line_array[5]
            fileTime = line_array[6]
            fileName = line_array[7]

            if '_SUCCESS' in fileName:
                continue
            if 'hive-staging' in fileName:
                processing = True
            if '_COPYING_' in fileName:
                processing = True

            fileSizeTotal += long(fileSize)
            fileNumTotal += 1

            # print fileSize, fileDate, fileTime, fileName
        break

    # 正在处理标识
    if processing:
        return False

    # 无文件
    if fileNumTotal == 0:
        return False

    # 检查文件总大小
    if check_file_size and fileSizeTotal < 100:
        return False

    return True

# HIVE表分区检查，多分区时，任一分区无数据都返回False
def has_hive_table_partition(table, partition_value):
    cmd = 'beeline --outputformat=csv2 -e "%s"'
    sql = "show partitions %s" % (table)
    retcode, outlines = executeShell(cmd % sql)
    if retcode != 0:
        return False

    ret = True
    # 有多个分区
    for line in outlines:
        # 过虑无关的分区
        if partition_value not in line: continue
        # print line

        # 多级分区拆分
        partition_array = line.split('/')

        # 拼接多级分区查询语句
        partition_qry_array = []
        for partition_str in partition_array:
            kv_array = partition_str.split('=')
            if len(kv_array) == 2:
                k = kv_array[0]
                v = kv_array[1]
                partition_qry_array.append("%s='%s'" % (k, v))
        if len(partition_qry_array) > 0:
            # 查看分区对应的HDFS路径
            sql = "describe formatted %s partition(%s)" % (table, ', '.join(partition_qry_array))
            # print sql
            retcode, outlines2 = executeShell(cmd % sql)
            if retcode != 0:
                return False
            for line in outlines2:
                if 'Location:' == line[:9]:
                    hdfs_path = line.split(',')[1]
                    # print hdfs_path
                    # 判断文件是否存在
                    if not has_hdfs_file(hdfs_path):
                        ret = False
                    break
    return ret


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
    # print finished_local_file('/home/pi/ts150/non_self/7_scheduler/hadoop_check_fun.sh')
    # print finished_local_file('/home/pi/ts150/non_self/7_scheduler/check.py')
    # print finished_local_file('/home/pi/ts150/non_self/7_scheduler/error.log')
    # print has_hdfs_file('/bigdata/input/TS150/p2log')
    print has_hive_table_partition('sor.ext_t0861_empe_h', '20170131')
    print has_hive_table_partition('sor.inn_t0861_empe_h', '20170131')
    # "describe extended sor.ext_t0861_empe_h partition(load_date='20170130');"

if __name__ == '__main__':
    main()