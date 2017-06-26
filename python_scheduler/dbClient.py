#!/usr/bin/env python
#coding:utf8

import os, sys, re
import sqlite3
import base64
import httplib, urllib
from var import *
from dbFun import *
sys.path.append("../python_common/")
import log
from common_fun import *


# 作业调度数据表操作Helper
class DbHelper:
    """作业调度数据表操作DbHelper"""
    def __init__(self):
        pass

    def __del__(self):
        pass

    # 通用查询执行
    def query_sql(self, sql, field_list=None):
        log.debug("query_sql:%s" % sql)

        ret = httpClient('select', sql)
        row_array = eval(ret)

        return row_array

    # 日志插入

    # 日志查询

    # 实体表插入
    def insert_entity(self, flag, entity, data_date, status=''):
        log.debug('insert_entity:[%s][%s][%s]' % (flag, entity, data_date))
        where = "where flag='%s' and entity='%s' and data_date='%s'" % \
                (flag, entity, data_date)

        # 记录不存在，则新增
        row_map = self.query_entity(where=where)

        if len(row_map) == 0:
            if not status:
                status = ''

            sql = "insert into entity(flag, entity, data_date, status) values('%s', '%s', '%s', '%s');" % \
                  (flag, entity, data_date, status)

            ret = httpClient('insert', sql)
            lastrowid = int(ret)

            return lastrowid

        return 0


    # 实体表查询
    def query_entity(self, id=0, where=''):
        log.debug('query_entity:[%d][%s]' % (id, where))

        if id > 0:
            where = 'where id=%d' % id
        sql = "select * from entity %s" % where

        ret = httpClient('select', sql)
        row_array = eval(ret)

        row_map = {}
        for row in row_array:
            row_map[row['id']] = row

        return row_map

    # 实体表更新
    def update_entity_from_id(self, id, status=None, pid=0, file_num=None, file_size=None, partition_num=None, record_num=None):
        log.debug('update_entity_from_id:[%s][%s]' % (id, status))

        update_field_array = ["ts=datetime('now', 'localtime')", ]
        if status:
            update_field_array.append("status='%s'" % status)
        if pid > 0:
            update_field_array.append("pid=%d" % pid)
        if file_num:
            update_field_array.append("status=%s" % file_num)
        if file_size:
            update_field_array.append("status=%s" % file_size)
        if partition_num:
            update_field_array.append("status=%s" % partition_num)
        if record_num:
            update_field_array.append("status=%s" % record_num)

        sql = "update entity set %s where id=%d" % (', '.join(update_field_array), id)
        ret = httpClient('update', sql)
        rowcount = int(ret)

        return rowcount


    # 实体表更新
    def update_entity(self, flag, entity, data_date, status=None, pid=0, file_num=None, file_size=None, partition_num=None, record_num=None):
        log.debug('update_entity:[%s][%s][%s]' % (flag, entity, data_date))
        where = "where flag='%s' and entity='%s' and data_date='%s'" % \
                (flag, entity, data_date)

        row_map = self.query_entity(where=where)

        # 如果记录不存在，则新增
        if len(row_map) == 0:
            self.insert_entity(flag, entity, data_date, status)

        updated_num = 0
        for row in row_map.values():
            # print sql
            self.update_entity_from_id(row['id'], status, pid, file_num, file_size, partition_num, record_num)

            updated_num += 1

        return updated_num


    # 作业配置表插入
    def insert_work_config(self, scriptName, options, start_date, end_date, priority, hostname, username):
        options_base64 = base64.b64encode(str(options))
        sql = "insert into work_config(script, options, start_date, end_date, priority, hostname, username) values('%s', '%s', '%s', '%s', %d, '%s', '%s');" % \
                (scriptName, options_base64, start_date, end_date, priority, hostname, username)
        # print sql
        ret = httpClient('insert', sql)
        lastrowid = int(ret)

        return lastrowid


    # 作业配置表查询
    def query_work_config(self, id=0, where=''):
        if id > 0:
            where = 'where id=%d' % id
        sql = "select * from work_config %s" % where

        ret = httpClient('select', sql)
        row_array = eval(ret)

        row_map = {}
        for row in row_array:
            options_str = base64.b64decode(row['options'])
            row['options'] = eval(options_str)
            row_map[row['id']] = row

        return row_map


    # 作业配置表更新
    def update_work_config(self, id, over_date=None, status=None, pid=0, end_date=None, options=None):
        where = 'where id=%d' % id
        update_field_array = ["ts=datetime('now', 'localtime')", ]
        if over_date:
            update_field_array.append("over_date='%s'" % over_date)
        if status:
            update_field_array.append("status='%s'" % status)
        if pid > 0:
            update_field_array.append("pid=%d" % pid)
        if end_date:
            update_field_array.append("end_date='%s'" % end_date)
        if options:
            options_base64 = base64.b64encode(str(options))
            update_field_array.append("options='%s'" % options_base64)

        if len(update_field_array) > 0:
            sql = "update work_config set %s %s" % (', '.join(update_field_array), where)
            # print sql
            ret = httpClient('update', sql)
            rowcount = int(ret)

            return rowcount
        else:
            return 0


    # 作业配置表删除
    def delete_work_config(self, id=0, where=''):
        if id > 0:
            where = 'where id=%d' % id
        sql = "delete from work_config %s" % where
        ret = httpClient('delete', sql)
        rowcount = int(ret)

        return rowcount

    # 任务表插入

    # 任务表查询


    # 定时作业配置表查询
    def query_crontab_config(self, id=0, where=''):
        if id > 0:
            where = 'where id=%d' % id
        sql = "select * from crontab_config %s" % where

        ret = httpClient('select', sql)
        row_array = eval(ret)

        row_map = {}
        for row in row_array:
            options_str = base64.b64decode(row['options'])
            row['options'] = eval(options_str)
            row_map[row['id']] = row

        return row_map


    # 定时作业配置表保存
    def save_crontab_config(self, id=0, crontab=None, script=None, options=None, status=None, pid=0, hostname=None, username=None):

        where = "where crontab='%s' and script='%s'" % (crontab, script)
        row_map = self.query_crontab_config(id, where)

        if len(row_map) == 0:
            options_base64 = base64.b64encode(str(options))
            sql = "insert into crontab_config(crontab, script, options, hostname, username) values('%s', '%s', '%s', '%s', '%s');" % \
                  (crontab, script, options_base64, hostname, username)
            ret = httpClient('insert', sql)
            lastrowid = int(ret)

            return lastrowid
        else:
            if id > 0:
                where = 'where id=%d' % id
            update_field_array = ["ts=datetime('now', 'localtime')", ]
            if crontab:
                update_field_array.append("crontab='%s'" % crontab)
            if script:
                update_field_array.append("script='%s'" % script)
            if options:
                options_base64 = base64.b64encode(str(options))
                update_field_array.append("options='%s'" % options_base64)
            if status:
                update_field_array.append("status='%s'" % status)
            if pid > 0:
                update_field_array.append("pid=%d" % pid)
            if hostname:
                update_field_array.append("hostname='%s'" % hostname)
            if username:
                update_field_array.append("username='%s'" % username)

            if len(update_field_array) > 0:
                sql = "update crontab_config set %s %s" % (', '.join(update_field_array), where)
                # print sql
                ret = httpClient('update', sql)
                rowcount = int(ret)

                return rowcount


def httpClient(operate, sql):
    client = None
    try:
        params = urllib.urlencode({'sql':sql})
        headers = {"Content-type": "application/x-www-form-urlencoded",
                   "Accept": "text/plain"}

        client = httplib.HTTPConnection(http_ip, http_port, timeout=30)
        client.request("POST", "/sql/%s" % operate, params, headers)

        response = client.getresponse()
        httpBody = response.read()
        if response.status == 200:
            return httpBody
        else:
            log.error(sql, logFile='httpSql')
            log.error(response.reason, logFile='httpSql')
            log.error(httpBody, logFile='httpSql')
            raise CommonError(msg='http请求sql执行:%s\n%s\n%s' % (sql, response.reason, httpBody))
        # print response.getheaders() #获取头信息
    except Exception, e:
        print e
    finally:
        if client:
            client.close()


def main():
    db = DbHelper()
    # print db.query_entity()
    print db.insert_entity(flag='test2', entity='T1', data_date='20170621', status='resume')
    print db.query_entity(where="where flag='test'")
    print db.update_work_config(9, '20171212', 'processing')
    print db.query_entity(1)
    print db.query_entity(where="where data_date='20170101'")
    print db.update_entity_from_id(1, 'exists')
    print db.update_entity(flag='test2', entity='T1', data_date='20170621', status='exists')
    print db.query_entity(1)
    print db.query_entity(where="where status<>'exists' order by data_date")
    # print db.query_sql('select * from entity ')
    # res_array = db.query_sql('select flag, entity, min(data_date) as min_data_date from entity group by flag, entity')
    # for row in res_array:
    #     print row

    pass


if __name__ == '__main__':
    sql = 'select flag, entity, min(data_date) as min_data_date from entity group by flag, entity'
    # httpClient('select', sql)

    # sql = "insert into log(msg, ret) values('Hello World', 1)"
    # httpClient('insert', sql)
    # sql = "update log set msg='Hello'"
    # httpClient('update', sql)
    # sql = "delete from log where ret=1"
    # httpClient('delete', sql)
    main()
    # print sys.argv
    # for var in getVar(sys.argv[1]):
    #     print var
