#!/usr/bin/env python
#coding:utf8

import os, sys
import sqlite3
from var import *
from baseFun import *
sys.path.append("../python_common/")
import log
from common_fun import *


# 作业调度数据表操作Helper
class DbHelper:
    """作业调度数据表操作DbHelper"""
    def __init__(self):
        # 数据库文件不存在，则新建库
        self.dbFileName = r"%s/%s.db" % (run_path, app)

        hasDb = os.path.exists(self.dbFileName)
        # log.debug("库%s是否存在:%s" % (dbFileName, hasDb))

        self.conn = sqlite3.connect(self.dbFileName)
        if not hasDb:
            createDbFileName = r"./db.sql"
            if os.path.exists(createDbFileName):
                for sql in getSql(createDbFileName):
                    self.conn.execute(sql)
                self.conn.close()
                self.conn = sqlite3.connect(self.dbFileName)
            else:
                sys.stderr.write("数据库不存在，建表文件:[%s]也不存在\n" % createDbFileName)
                exit(1)

        # 默认返回的数据是Unicode，改为str可适应utf8与gbk
        self.conn.text_factory = str
        self.conn.isolation_level = None
        self.cur = self.conn.cursor()


    def __del__(self):
        if "conn" in dir(self):
            self.cur.close()
            self.conn.commit()
            self.conn.close()

        chmod(self.dbFileName, 0777)


    # 取SQL select执行结果字段名列表
    def get_field_list(self, description=None):
        field_list = []
        if not description:
            description = self.cur.description

        for field in description:
            field_list.append(field[0])

        return field_list


    # 通用查询执行
    def query_sql(self, sql):
        log.debug("query_sql:%s" % sql)

        self.cur.execute(sql)
        res = self.cur.fetchall()
        field_list = self.get_field_list()

        row_array = []
        for row in res:
            # print row
            column_map = dict(zip(field_list, row))
            row_array.append(column_map)

        return row_array


def main():
    db = DbHelper()
    db.query_sql('select * from work_config limit 1')


if __name__ == '__main__':
    main()
