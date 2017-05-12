#!/usr/bin/python
#coding:utf8

import os, sys, re
from datetime import *
from var import *
from log import *
from common_fun import *

# 按空格当分隔符
space_pattern = re.compile(r'\s+')

# 条件、结果检查类

        
# GP 流水分区表检查

# 获取GP表或分区属性
def get_gp_table_attribute(table, partition_value=None):
    pass
    
# GP 拉连表检查


# 检查HDFS文件是否存在
def has_hdfs_file(path, check_file_size=True):
    try:
        (processing, fileNumTotal, fileSizeTotal) = get_hdfs_file_attribute(path)
    except CommonError:
        return False

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


# 返回HDFS文件属性
def get_hdfs_file_attribute(path):
    fileSizeTotal = 0
    fileNumTotal = 0
    processing = False

    cmd = 'hadoop fs -ls -R "%s"' % path
    print cmd
    print "==========================================="
    for i in range(3):
        retcode, outlines = executeShell(cmd)
        if retcode != 0:
            log.error('hadoop fs命令执行失败:[%s][%d]' % (cmd, retcode))
            continue
        for line in outlines:
            # 目录不计入文件
            if len(line) == 0 or line[0] == 'd':
                continue
            # ls内容按列分割
            line_array = re.split(space_pattern, line)
            fileSize = line_array[4]
            # fileDate = line_array[5]
            # fileTime = line_array[6]
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
    else:
        raise CommonError(msg='hadoop ls命令出错:%s' % cmd)

    return (processing, fileNumTotal, fileSizeTotal)


# HDFS目录检查,支持目录名后带表达式：(file_num>1 and file_size>=1024) or True
def valid_hdfs_file(path, data_date=None):
    if ':' in path:
        patt = re.compile(r'([\S+]+):\{(.*)\}')
        m = patt.match(path)
        if m:
            # print m.groups()
            path = m.group(1)
            express = m.group(2).replace(', ', ' and ')
        else:
            raise CommonError(msg="HDFS表检查表达式有误:%s" % table)
    else:
        express = 'file_num>=1 and file_size>=100'

    try:
        if data_date:
            path = '%s/%s' % (path, data_date)

        (processing, file_num, file_size) = get_hdfs_file_attribute(path)

        # 正在处理标识
        if processing:
            return False

        # print 'processing:%s, file_num:%d, file_size:%d' % (processing, file_num, file_size)
        # 检查条件脚本执行
        if not eval(express):
            return False

    except CommonError:
        return False
    except Exception as e:
        raise CommonError(msg="HDFS检查表达式解释有误:%s,%s" % (express, e))

    return True


# 检查HIVE表分区是否存在，支持检查文件大小与记录数，多分区时，任一分区无数据都返回False
def has_hive_table_partition(table, partition_value, check_file_size=True, check_record_num=False):
    ret = False
    ret_attribute_array = get_hive_table_attribute(table, check_record_num, partition_value)
    for (processing, fileNumTotal, fileSizeTotal, record_num) in ret_attribute_array:
        if processing:
            return False
        if fileNumTotal == 0:
            return False
        if check_file_size and fileSizeTotal < 100:
            return False
        if check_record_num and record_num == 0:
            return False
        # 存在分区，且满足所有条件
        ret = True

    return ret


# HIVE表检查,支持表名后带表达式：(file_num>1 and file_size>=1024, partition_num==1, record_num>100) or True
def valid_hive_table(table, partition_value=None):
    ret = False
    check_record_num = False
    if 'record_num' in table:
        check_record_num = True

    if ':' in table:
        patt = re.compile(r'([\.\w]+):\{(.*)\}')
        m = patt.match(table)
        if m:
            # print m.groups()
            table = m.group(1)
            express = m.group(2).replace(', ', ' and ')
        else:
            raise CommonError(msg="Hive表检查表达式有误:%s" % table)
    else:
        express = 'file_num>=1 and file_size>=100'

    ret_attribute_array = get_hive_table_attribute(table, check_record_num, partition_value)
    partition_num = len(ret_attribute_array)

    for (processing, file_num, file_size, record_num) in ret_attribute_array:
        if processing:
            return False

        try:
            # print 'processing:%s, file_num:%d, file_size:%d, record_num:%d, partition_num:%d' % (processing, file_num, file_size, record_num, partition_num)
            # 检查条件脚本执行
            if not eval(express):
                return False
        except Exception as e:
            raise CommonError(msg="Hive表检查表达式解释有误:%s,%s" % (express, e))
            
        ret = True

    return ret


# 获取Hive表记录数
def get_hive_record_num(table, where=None):
    cmd = 'beeline --outputformat=vertical --slient=true -e "%s"'
    sql = "select count(*) as count_num from %s " % table
    if where:
        sql += "where %s" % where

    retcode, outlines = executeShell_ex(cmd % sql)
    for line in outlines:
        if 'count_num' in line:
            # print '[%s]' % line
            line_array = re.split(space_pattern, line)
            record_num = int(line_array[1])
            return record_num


# 获取HIVE表或分区属性
def get_hive_table_attribute(table, get_record_num=False, partition_value=None):
    (processing, fileNumTotal, fileSizeTotal, record_num) = (True, 0, 0, 0)
    ret_attribute_array = []

    cmd = 'beeline --outputformat=csv2 -e "%s"'

    # 无分区，直接查Hive表
    if not partition_value:
        sql = "describe formatted %s" % table

        retcode, outlines = executeShell_ex(cmd % sql)
        for line in outlines:
            if 'Location:' == line[:9]:
                hdfs_path = line.split(',')[1]
                # print hdfs_path

                # 获取HDFS文件属性
                (processing, fileNumTotal, fileSizeTotal) = get_hdfs_file_attribute(hdfs_path)

                break

        # 状态正常，且需要查记录数
        if get_record_num and not processing and fileNumTotal > 0 and fileSizeTotal > 0:
            record_num = get_hive_record_num(table)

        ret_attribute_array.append((processing, fileNumTotal, fileSizeTotal, record_num)) 

    else:
        sql = "show partitions %s" % (table)
        retcode, outlines = executeShell_ex(cmd % sql)

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
                retcode, outlines2 = executeShell_ex(cmd % sql)
                for line in outlines2:
                    if 'Location:' == line[:9]:
                        hdfs_path = line.split(',')[1]
                        # print hdfs_path
                        # 获取HDFS文件属性
                        (processing, fileNumTotal, fileSizeTotal) = get_hdfs_file_attribute(hdfs_path)
                        break

                # 状态正常，且需要查记录数
                if get_record_num and not processing and fileNumTotal > 0 and fileSizeTotal > 0:
                    record_num = get_hive_record_num(table, ' and '.join(partition_qry_array))
                            
                ret_attribute_array.append((processing, fileNumTotal, fileSizeTotal, record_num)) 

    return ret_attribute_array


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
    # print get_hdfs_file_attribute('/bigdata/input/TS150/p2log')
    print valid_hdfs_file('/bigdata/input/TS150/p2log')
    print valid_hdfs_file('/bigdata/input/TS150/p2log:{file_num>100 and file_size>=1024}')
    print valid_hdfs_file('/bigdata/input/TS150/p2log:{file_num>1000 and file_size>=10240000}')
    # print get_hdfs_file_attribute('/bigdata2/input/TS150/p2log')
    # print has_hdfs_file('/bigdata/input/TS150/p2log')
    # print has_hive_table_partition('sor.ext_t0861_empe_h', '20170131')
    # print has_hive_table_partition('sor.inn_t0861_empe_h', '20170131')
    # "describe extended sor.ext_t0861_empe_h partition(load_date='20170130');"
    # print get_hive_table_attribute('sor.ext_t0861_empe_h', True, '20170131')
    # print get_hive_table_attribute('sor.inn_t0861_empe_h', True, '201701')
    # print get_hive_table_attribute('ts150.test_gbk_2', True)
    # print get_hive_table_attribute('ts150.test_gbk_5', True)
    # print get_hive_table_attribute('ts150.test_gbk_6', True)
    # print get_hive_table_attribute('ts150.test_gbk_2')
    print valid_hive_table("sor.inn_t0861_empe_h:{(file_num>1 and file_size>=1024, partition_num==1, record_num>100) or True}", '20170131')
    # print valid_hive_table("sor.inn_t0861_empe_h")


def testHiveCheck():
    IN_CUR_HIVE = "main_trade_flow_ccbs_atm_pos_ectip_card_user_a:{(file_num>1 and file_size>=1024, partition_num==1, record_num>100) or True}"
    # IN_CUR_HIVE="main_trade_flow_ccbs_atm_pos_ectip_card_user_a:{file_num>1, file_size>=1024, partition_num==1, record_num>100}"
    # IN_CUR_HIVE="main_trade_flow_ccbs_atm_pos_ectip_card_user_a:{file_size>=1024}"
    # IN_CUR_HIVE="main_trade_flow_ccbs_atm_pos_ectip_card_user_a"
    print IN_CUR_HIVE

    if ':' in IN_CUR_HIVE:
        patt = re.compile(r'(\w+):\{(.*)\}')
        m = patt.match(IN_CUR_HIVE)
        if m:
            print m.groups()
            express = m.group(2).replace(', ', ' and ')
            file_num = 2
            file_size = 1024
            partition_num = 1
            record_num = 120
            print eval(express)
        else:
            print 'match error'
    else:
        print IN_CUR_HIVE


if __name__ == '__main__':
    main()
