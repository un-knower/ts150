#!/usr/bin/env python
#coding:utf8

import os, sys
import sqlite3
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


# 作业调度数据表操作Helper
class DbHelper:
    """作业调度数据表操作DbHelper"""
    def __init__(self):
        # 数据库文件不存在，则新建库
        dbFileName = r"%s/%s.db" % (run_path, app)

        hasDb = os.path.exists(dbFileName)
        log.debug("库%s是否存在:%s" % (dbFileName, hasDb))
        
        self.conn = sqlite3.connect(dbFileName)
        
        if not hasDb or True:
            createDbFileName = r"%s/7_scheduler/db.sql" % (base_path)
            if os.path.exists(createDbFileName):
                for sql in getSql(createDbFileName):
                    self.conn.execute(sql)
                self.conn.close()
                self.conn = sqlite3.connect(dbFileName)
            else:
                sys.stderr.write("数据库不存在，建表文件:[%s]也不存在\n" % createDbFileName)
                exit(1)


    def __del__(self):
        if "conn" in dir(self):
            self.conn.close()


    # 日志插入

    # 日志查询

    # 实体表插入
    
    # 实体表查询
    
    # 作业配置表插入

    # 作业配置表查询

    # 任务表插入

    # 任务表查询


def main():
    db = DbHelper()
    pass


if __name__ == '__main__':
    main()