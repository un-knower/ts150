#!/usr/bin/env python
#coding:utf8

import os, sys
import sqlite3
import base64
from var import *
from log import *

# 获取SQL脚本文件中的SQL语句
def getSql(fileName):
    all_lines = ""
    if os.path.exists(fileName):
        f = open(fileName, "r")

        for line in f.readlines():
            stmt = line.split('--')[0]
            all_lines += stmt

        # for sql in all_lines.split(';'):
        #     self.conn.execute(sql)

        f.close()
    return all_lines.split(';')


# 获取SQL脚本文件中的注释语句中包含k-v
def getRemarkKV(fileName, remark_flag=None):
    kv_map = {}
    if os.path.exists(fileName):
        f = open(fileName, "r")

        for line in f.readlines():
            if remark_flag:
                line_array = line.split(remark_flag)
                if len(line_array) <= 1:
                    continue
                stmt = line_array[1]
            else:
                stmt = line
            kv_array = stmt.split('=')
            if len(kv_array) <= 1:
                continue
            kv_map[kv_array[0]] = kv_array[1].strip().replace('"', '').replace("'", "")

        f.close()
    return kv_map


# 作业调度数据表操作Helper
class DbHelper:
    """作业调度数据表操作DbHelper"""
    def __init__(self):
        # 数据库文件不存在，则新建库
        dbFileName = r"%s/%s.db" % (run_path, app)

        hasDb = os.path.exists(dbFileName)
        # log.debug("库%s是否存在:%s" % (dbFileName, hasDb))
        
        self.conn = sqlite3.connect(dbFileName)
        
        if not hasDb:
            createDbFileName = r"%s/7_scheduler/db.sql" % (base_path)
            if os.path.exists(createDbFileName):
                for sql in getSql(createDbFileName):
                    self.conn.execute(sql)
                self.conn.close()
                self.conn = sqlite3.connect(dbFileName)
            else:
                sys.stderr.write("数据库不存在，建表文件:[%s]也不存在\n" % createDbFileName)
                exit(1)
        self.conn.isolation_level = None
        self.cur = self.conn.cursor()


    def __del__(self):
        if "conn" in dir(self):
            self.cur.close()
            self.conn.commit()
            self.conn.close()


    # 取SQL select执行结果字段名列表
    def get_field_list(self, description=None):
        field_list = []
        if not description:
            description = self.cur.description

        for field in description:
            field_list.append(field[0])

        return field_list

    # 通用查询执行
    def query_sql(self, sql, field_list=None):
        log.debug("query_sql:%s" % sql)

        self.cur.execute(sql)
        res = self.cur.fetchall()
        field_list = self.get_field_list()

        row_array = []
        for row in res:
            column_map = dict(zip(field_list, row))
            row_array.append(column_map)

        return row_array

    # 日志插入

    # 日志查询

    # 实体表插入
    def insert_entity(self, flag, entity, data_date):
        log.debug('insert_entity:[%s][%s][%s]' % (flag, entity, data_date))
        where = "where flag='%s' and entity='%s' and data_date='%s'" % \
                (flag, entity, data_date)

        row_map = self.query_entity(where=where)

        if len(row_map) == 0:        
            sql = "insert into entity(flag, entity, data_date) values('%s', '%s', '%s');" % \
                    (flag, entity, data_date)
            # print sql
            self.cur.execute(sql)
            
            return self.cur.lastrowid
        return 0

    # 实体表查询
    def query_entity(self, id=0, where=''):
        log.debug('query_entity:[%d][%s]' % (id, where))

        if id > 0:
            where = 'where id = %d' % id
        sql = "select * from entity %s" % where
        self.cur.execute(sql)
        res = self.cur.fetchall()

        field_list = self.get_field_list()
        row_map = {}
        for row in res:
            column_map = dict(zip(field_list, row))
            row_map[column_map['id']] = column_map

        return row_map

    # 实体表更新
    def update_entity(self, id, status=None, pid=0):
        log.debug('update_entity:[%s][%s]' % (id, status))
        
        update_field_array = ["ts = datetime('now', 'localtime')", ]
        if status:
            update_field_array.append("status = '%s'" % status)
        if pid > 0:
            update_field_array.append("pid = %d" % pid)

        sql = "update entity set %s where id=%d" % (', '.join(update_field_array), id)
        log.debug(sql)
        self.cur.execute(sql)
            
        return self.cur.rowcount


    # 作业配置表插入
    def insert_work_config(self, scriptName, options, start_date, end_date, priority):
        options_base64 = base64.b64encode(str(options))
        sql = "insert into work_config(script, options, start_date, end_date, priority) values('%s', '%s', '%s', '%s', %d);" % \
                (scriptName, options_base64, start_date, end_date, priority)
        # print sql
        self.cur.execute(sql)
        
        return self.cur.lastrowid

    # 作业配置表查询
    def query_work_config(self, id=0, where=''):
        if id > 0:
            where = 'where id = %d' % id
        sql = "select * from work_config %s" % where
        self.cur.execute(sql)
        res = self.cur.fetchall()

        field_list = self.get_field_list()
        row_map = {}
        for row in res:
            column_map = dict(zip(field_list, row))
            column_map['options'] = base64.b64decode(column_map['options'])
            row_map[column_map['id']] = column_map

        return row_map

    # 作业配置表更新
    def update_work_config(self, id, over_date=None, status=None, pid=0):
        where = 'where id = %d' % id
        update_field_array = ["ts = datetime('now', 'localtime')", ]
        if over_date:
            update_field_array.append("over_date = '%s'" % over_date)
        if status:
            update_field_array.append("status = '%s'" % status)
        if pid > 0:
            update_field_array.append("pid = %d" % pid)

        if len(update_field_array) > 0:
            sql = "update work_config set %s %s" % (', '.join(update_field_array), where)
            # print sql
            self.cur.execute(sql)

            return self.cur.rowcount
        else:
            return 0

    # 任务表插入

    # 任务表查询


def main():
    db = DbHelper()
    # print db.update_work_config(1, '20171212', 'processing')
    # print db.query_entity(1)
    # print db.query_entity(where="where data_date='20170101'")
    # print db.update_entity(1, 'exists')
    # print db.query_entity(1)
    # print db.query_entity(where="where status<>'exists' order by data_date")
    # print db.query_sql('select * from entity ')
    print db.query_sql('select flag, entity, min(data_date) as min_data_date from entity group by flag, entity')
    pass


if __name__ == '__main__':
    main()