#!/usr/bin/python
#coding:utf8

import os, sys, re
import datetime
from var import *
from log import *
from common_fun import *

# 按空格当分隔符
space_pattern = re.compile(r'\s+')

# 条件、结果检查类

        
# GP SQL 执行
def psql(schema, table_name, sql):
    if schema in ('base', 'base_pro', 'aca_xk_view'):
        if is_test:
            cmd = 'psql -x -h 128.196.116.69 -d sordb_pl3 -U aca_xk_user -c "%s"'
        else:
            cmd = 'export HOME=/home/ap/dip;source $HOME/.bash_profile;export PGPASSWORD=password; psql -x -h 11.36.156.168 -d sordb -U aca_xk_user -c "%s"'
    else:
        if is_test:
            cmd = 'psql -x -h 128.196.116.70 -d acadb_pl2 -U aca_siam_etl -c "%s"'
        else:
            cmd = 'export HOME=/home/ap/dip;source $HOME/.bash_profile;psql -x -h 11.58.112.141 -d saldb -U aca_siam_etl -c "%s"'

    log.debug(sql)
    retcode, outlines = executeShell(cmd % sql)
    if retcode != 0:
        log.error('GP命令执行失败:[%s][%d]' % (cmd % sql, retcode))
        return []

    out_array = []
    for line in outlines:
        if len(line) == 0: continue

        # print '[%s]' % line
        line_array = line.split('|')

        if len(line_array) == 1:
            if 'RECORD' in line:
                out_dict = {}
                out_array.append(out_dict)
            continue

        line_array = [x.strip() for x in line_array]

        out_dict[line_array[0]] = line_array[1]

    log.debug(out_array)
    return out_array


# 获取GP表属性
def get_gp_table_attribute(table, check_date=None):
    log.info('开始运行--获取GP表属性:[%s][%s]' % (table, check_date), 'check_gp')

    # 表名分解，schema.table
    table_array = table.lower().split('.')
    assert len(table_array) == 2

    schema = table_array[0]
    table_name = table_array[1]
    table_type = table_name[-2:]

    assert table_type in ('_a', '_h')

    if table_type == '_a':
        date_field_name = 'p9_data_date'
    elif table_type == '_h':
        date_field_name = 'p9_start_date'

    is_partition_table = False

    # 表是否存在
    table_rows = psql(schema, table_name, "select * from pg_tables where schemaname='%s' and tablename='%s'" % (schema, table_name))
    if len(table_rows) == 1:
        # 表是否分区
        partition_rows = psql(schema, table_name, "select * from pg_partitions where schemaname='%s' and tablename='%s'" % (schema, table_name))
        if len(partition_rows) >= 1:
            is_partition_table = True
            # 找分区字段名
            sql = "select * from pg_partition_columns where schemaname='%s' and tablename='%s' and position_in_partition_key=1" % (schema, table_name)
            partition_column_rows = psql(schema, table_name, sql)
            date_field_name = partition_column_rows[0]['columnname']
    else:
        # 视图是否存在
        view_rows = psql(schema, table_name, "select * from pg_views where schemaname='%s' and viewname='%s'" % (schema, table_name))
        if len(view_rows) == 0:
            return (False, check_date, 0)

    log.info('找到GP表或分区:[%s][%s][%s]' % (table, is_partition_table, date_field_name), 'check_gp')

    # 取最新日期检查
    if not check_date:
        # 取最大分区
        if is_partition_table:
            sql = "select max(partitionrangestart) from pg_partitions where schemaname='%s' and tablename='%s')" % (schema, table_name)
            max_partition_rows = psql(schema, table_name, sql)
            if len(max_partition_rows) == 1:
                check_date = max_partition_rows[0]['max']
            else:
                return (False, check_date, 0)
        else:
            # 非分区表取最大日期当检查日期
            sql = "select max(%s) from %s.%s" % (date_field_name, schema, table_name)
            max_date_rows = psql(schema, table_name, sql)
            if len(max_date_rows) == 1:
                check_date = max_date_rows[0]['max']

    if check_date == '':
        return (False, check_date, 0)

    log.info('确认GP表检查日期:[%s][%s]' % (table, check_date), 'check_gp')

    # 按指定日期检查记录数
    if is_partition_table:
        sql = "select * from pg_partitions where schemaname='%s' and tablename='%s' and partitionrangestart<=date'%s' and partitionrangeend>date'%s')" % (schema, table_name, check_date, check_date)
        partition_rows = psql(schema, table_name, sql)
        if len(partition_rows) == 1:
            table_name = partition_rows[0]['partitiontablename']
        else:
            return (False, check_date, 0)
    
    # 获取记录数
    sql = "select count(*) from %s.%s where %s=date'%s'" % (schema, table_name, date_field_name, check_date)
    # log.info(sql, 'check_gp')
    count_rows = psql(schema, table_name, sql)
    
    log.info('完成--获取GP表属性:[%s][%s][%s]' % (table, check_date, count_rows[0]['count']), 'check_gp')

    return (True, check_date, int(count_rows[0]['count']))


# GP表检查,支持表名后带表达式：record_num>100
def valid_gp_table(table, check_date):
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
        express = 'record_num>=1'

    (hasTable, real_check_date, record_num) = get_gp_table_attribute(table, check_date)

    if not hasTable:
        log.info('GP表或分区不存在:[%s][%s]' % (table, check_date), 'check_gp')
        return False

    try:
        # 检查条件脚本执行
        if not eval(express):
            log.info('表达式检查未通过:[%s][%s][%s]' % (table, check_date, express), 'check_gp')
            return False
    except Exception as e:
        raise CommonError(msg="GP表检查表达式解释有误:%s,%s" % (express, e))

    log.info('GP表检查通过:[%s][%s][%s]' % (table, check_date, express), 'check_gp')

    return True


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
    # print cmd
    # print "==========================================="
    for i in range(3):
        log.info(cmd, 'check_hdfs')
        retcode, outlines = executeShell(cmd)
        if retcode != 0:
            log.error('hadoop fs命令执行失败:[%s][%d]' % (cmd, retcode))
            continue
        for line in outlines:
            # 目录不计入文件
            if len(line) == 0 or line[0] == 'd':
                continue
            log.info(line, 'check_hdfs')
            
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
        log.info('hadoop ls命令出错:%s' % cmd, 'check_hdfs')
        raise CommonError(msg='hadoop ls命令出错:%s' % cmd)

    log.info('hdfs:[%s][%s][%s]' % (processing, fileNumTotal, fileSizeTotal), 'check_hdfs')

    return (processing, fileNumTotal, fileSizeTotal)


# HDFS目录检查,支持目录名后带表达式：(file_num>1 and file_size>=1024) or True
def valid_hdfs_file(path, data_date=None):
    if ':' in path:
        patt = re.compile(r'(\S+):\{(.*)\}')
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
        log.info("HDFS检查表达式解释有误:%s,%s" % (express, e), 'check_hdfs')
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
    cmd = 'beeline --outputformat=vertical -e "%s"'
    sql = "select count(*) as count_num from %s " % table
    if where:
        sql += "where %s" % where

    log.info(cmd % sql, 'check_hive')
    retcode, outlines = executeShell_ex(cmd % sql)
    for line in outlines:
        if 'count_num' in line:
            # print '[%s]' % line
            line_array = re.split(space_pattern, line)
            record_num = int(line_array[1])
            log.info('record_num:[%s]' % record_num, 'check_hive')

            return record_num


# 获取HIVE表或分区属性
def get_hive_table_attribute(table, get_record_num=False, partition_value=None):
    (processing, fileNumTotal, fileSizeTotal, record_num) = (True, 0, 0, 0)
    ret_attribute_array = []

    cmd = 'beeline --outputformat=csv2 -e "%s"'
    log.info(cmd, 'check_hive')

    # 无分区，直接查Hive表
    if not partition_value:
        sql = "describe formatted %s" % table

        retcode, outlines = executeShell_ex(cmd % sql)
        for line in outlines:
            if 'Location:' == line[:9]:
                hdfs_path = line.split(',')[1]
                log.info(hdfs_path, 'check_hive')
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
        log.info(sql, 'check_hive')
        retcode, outlines = executeShell_ex(cmd % sql)

        # 有多个分区
        for line in outlines:
            # 过虑无关的分区
            if partition_value not in line: continue
            # print line
            log.info(line, 'check_hive')

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
                log.info(sql, 'check_hive')
                # print sql
                retcode, outlines2 = executeShell_ex(cmd % sql)
                for line in outlines2:
                    if 'Location:' == line[:9]:
                        hdfs_path = line.split(',')[1]
                        log.info(hdfs_path, 'check_hive')
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


# 获取目录下对应的文件列表
def get_local_file_attribute(pathName, data_date=None):
    log.info('获取本地文件属性:%s' % pathName, 'check_local')
    if isLinux():
        cmd = 'ls'
    else:
        cmd = 'dir'
        pathName = pathName.replace('/', os.path.sep)

    if data_date: 
        pathName = pathName.replace('${date}', data_date).replace('${data_date}', data_date).replace('${log_date}', data_date)

    log.info('变更替换后本地文件属性:%s' % pathName, 'check_local')

    cmd += ' %s' % pathName
    searchPath, searchFile = os.path.split(pathName)

    file_list = []
    retcode, outlines = executeShell(cmd)
    if retcode == 0:
        for line in outlines:
            fileName = line.strip()
            resultPath, resultFile = os.path.split(fileName)
            if searchPath != '' and resultPath == '':
                fileName = searchPath + os.path.sep + resultFile
            # print fileName
            stat = os.stat(fileName)
            # print stat
            processing = True

            fileModiftDateTime = datetime.datetime.fromtimestamp(stat.st_mtime)
            interval = datetime.datetime.now() - fileModiftDateTime

            if interval.days>0 or interval.seconds>30*60:
                processing = False

            file_list.append((fileName, processing, stat.st_size))
    else:
        pass

    return file_list


# 本地文件夹检查,支持表达式：(file_num>1 and file_size>=1024 and total_file_size>10000 and processing==False)
def valid_local_file(pathName, data_date=None):
    log.info('检查本地文件属性:[%s][%s]' % (pathName, data_date), 'check_local')

    if ':' in pathName:
        patt = re.compile(r'([\S]+):\{(.*)\}')
        m = patt.match(pathName)
        if m:
            # print m.groups()
            pathName = m.group(1)
            express = m.group(2).replace(', ', ' and ').replace(',', ' and ')
        else:
            raise CommonError(msg="本地文件检查表达式有误:%s" % pathName)
    else:
        express = 'file_num>=1 and file_size>=100 and processing==False'

    ret_file_array = get_local_file_attribute(pathName, data_date)
    file_num = len(ret_file_array)

    total_file_size = 0
    for (fileName, processing, file_size) in ret_file_array:
        total_file_size += file_size

    for (fileName, processing, file_size) in ret_file_array:
        try:
            # print 'processing:%s, file_num:%d, file_size:%d, total_file_size:%d' % (processing, file_num, file_size, total_file_size)
            # 检查条件脚本执行
            if not eval(express):
                return False
        except Exception as e:
            raise CommonError(msg="本地文件检查表达式解释有误:%s,%s" % (express, e))

    log.info('检查本地文件属性:[%s][%s]通过' % (pathName, data_date), 'check_local')

    return True


# 本地文件FTP传输完成检查
def finished_local_file(fileName):
    if not os.path.exists(fileName):
        return False

    # 判断文件停止修改30分钟以上
    stat = os.stat(fileName)
    fileModiftDateTime = datetime.datetime.fromtimestamp(stat.st_mtime)
    interval = datetime.datetime.now() - fileModiftDateTime
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
    # print get_local_file_attribute('*.py')
    # print get_local_file_attribute('./*.py')
    # print get_local_file_attribute('/home/ap/dip/file/archive/input/ts150/000000000/data')
    # print get_local_file_attribute('/home/ap/dip/file/archive/input/ts150/000000000/data/${date}', '20170227')
    # print get_local_file_attribute('/home/ap/dip/file/archive/input/ts150/000000000/data/${date}/', '201?0227')
    # print get_local_file_attribute('/home/ap/dip/file/archive/input/ts150/000000000/data/P2/${date}/*.gz', '20160506')
    print valid_local_file('/home/ap/dip/file/archive/input/ts150/000000000/data/P2/${date}/*.dat', '20160506')
    print valid_local_file('/home/ap/dip/file/archive/input/ts150/000000000/data/P2/${date}/*.gz:{total_file_size>10000}', '20160506')
    # print valid_hdfs_file('/bigdata/input/TS150/p2log')
    # print valid_hdfs_file('/bigdata/input/TS150/p2log:{file_num>100 and file_size>=1024}')
    # print valid_hdfs_file('/bigdata/input/TS150/p2log:{file_num>1000 and file_size>=10240000}')
    # print get_hdfs_file_attribute('/bigdata2/input/TS150/p2log/${date}/*.gz')
    # print get_hdfs_file_attribute('/bigdata2/input/TS150/p2log/*.${date}*.gz')
    # print has_hdfs_file('/bigdata/input/TS150/p2log')
    # print has_hive_table_partition('sor.ext_t0861_empe_h', '20170131')
    # print has_hive_table_partition('sor.inn_t0861_empe_h', '20170131')
    # print get_hive_table_attribute('sor.ext_t0861_empe_h', True, '20170131')
    # print get_hive_table_attribute('sor.inn_t0861_empe_h', True, '201701')
    # print get_hive_table_attribute('ts150.test_gbk_2', True)
    # print get_hive_table_attribute('ts150.test_gbk_5', True)
    # print get_hive_table_attribute('ts150.test_gbk_6', True)
    # print get_hive_table_attribute('ts150.test_gbk_2')
    # print valid_hive_table("sor.inn_t0861_empe_h:{(file_num>1 and file_size>=1024, partition_num==1, record_num>100) or True}", '20170131')
    # print valid_hive_table("sor.inn_t0861_empe_h")
    # print get_gp_table_attribute('app_siam.t1000_crcrd_h')
    # print get_gp_table_attribute('base.t1000_crcrd_a')

    # print get_gp_table_attribute('app_siam.toddc_Saacnacn_H')
    # print get_gp_table_attribute('app_siam.ip_area_full')
    # print get_gp_table_attribute('base.toddc_Saacnacn_H', '20160725')


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
