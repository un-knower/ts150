#!/usr/bin/env python
#coding:utf8

import os, sys
import dbHelper
from baseFun import *
sys.path.append("../python_common/")
import log
from common_fun import *


def hdfs_file(store_server, file_path):
    if store_server == 'ts150_hdfs':
        # 判断登录状态
        # klist
        execute_command('klist')

    if store_server == 'p9_hdfs':
        # 判断登录状态
        # klist
        execute_command('klist')

    # 执行命令
    cmd_text = 'hadoop fs -ls -R "%s"' % file_path
    (return_code, out_lines, err_lines) = execute_command(cmd_text)
    if return_code != 0:
        print 'err'
    else:
        print out_lines
        print err_lines

    # 解释返回结果
    patt = re.compile('\s+')
    filestatus = 0
    processing = False
    for line in out_lines:
        field_array = patt.split(line)
        if len(field_array) > 5:
            file_permission = field_array[0]
            file_size = field_array[4]
            file_mtime = '%s %s' % (field_array[5], field_array[6])
            file_path = field_array[7]

            if '_SUCCESS' in file_path:
                continue
            if 'hive-staging' in file_path:
                processing = True
            if '_COPYING_' in file_path:
                processing = True
            if '_temporary' in file_path:
                processing = True

            if processing:
                filestatus = '1'
            elif file_size < 100:
                filestatus = '2'

            
            print(
                [file_permission, file_size, file_mtime, file_path, filestatus])
    # return out_res


def main():
    hdfs_file('ts150_hdfs', '/nisalog/20180403')

if __name__ == '__main__':
    main()