#!/usr/bin/env python
#coding:utf8

import os, sys
import dbHelper
import check_file
from baseFun import *
sys.path.append("../python_common/")
import log
from common_fun import *

class Monitor:
    def __init__(self):
       self.db = dbHelper.DbHelper() 

    def get_business_date(self, business_type):
        '''获取该业务的监控数据日期'''
        rows = self.db.select("select count(1) as count, max(data_date) as max_data_date from tbl_bussiness_date where bussiness_type='%s'" % business_type)
        row = rows[0]
        print row
        if row["count"] == 0:
            data_date = dateCalc(today(), -1)
            bussiness_date_id = self.db.insert("insert into tbl_bussiness_date(bussiness_type, data_date) values('%s', '%s')" % (business_type, data_date))
        else:
            data_date = row["max_data_date"]
            rows = self.db.select("select bussiness_date_id from tbl_bussiness_date where bussiness_type='%s' and data_date = '%s'" % (business_type, data_date))

            bussiness_date_id = rows[0]["bussiness_date_id"]

        return bussiness_date_id, data_date

    def save_monitor_file(self):
        pass

    def nisalog_monitor(self):
        # 获取日期
        bussiness_date_id, data_date = self.get_business_date('nisalog')
        self.hdfs_check(bussiness_date_id, 'ts150_hadoop', '/nisalog/%s' % data_date)

    def hdfs_check(self, bussiness_date_id, store_server, file_path):
        # 查询数据
        cmd_text, exit_code, file_attribute_array = check_file.hdfs_file('ts150_hadoop', store_server, file_path)

        # 写入监控动作
        monitor_action_id = self.db.insert("insert into tbl_monitor_action(bussiness_date_id, store_type, store_server, cmd_text, exit_code) values('%s', 'hdfs', 'ts150_hadoop', '%s', '%s')" % (bussiness_date_id, cmd_text, exit_code))

        # 写入监控结果
        if exit_code == 0:
            for file_attribute in file_attribute_array:
                (file_permission, file_size, file_mtime, file_path, file_status) = file_attribute

                self.db.insert("insert into tbl_monitor_file(monitor_action_id, file_path, file_status, file_size, file_mtime, file_permission) values('%s', '%s', '%s', %s, '%s', '%s')" % (monitor_action_id, file_path, file_status, file_size, file_mtime, file_permission))

def main():
    monitor = Monitor()
    # print monitor.get_business_date('nisalog')
    monitor.nisalog_monitor()


if __name__ == '__main__':
    main()
