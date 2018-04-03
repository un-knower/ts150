#! encoding=utf8
#!/usr/bin/env python

import os
import datetime
import platform
import unittest
import re
import sqlite3
import logging
import time
import optparse
try:
    import commands
except:
    import subprocess as commands

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(asctime)s: %(message)s', datefmt="%Y%m%d %H:%M:%S")
logger = logging.getLogger(__name__)
check_date = datetime.datetime.now().strftime('%Y%m%d %H:%M:%S')
tablename = 'banks_data'

def is_valid_date(str):
    try:
        time.strptime(str, "%Y%m%d")
        return True
    except:
        return False

class conn_sqlite():
    init_sql = """create table %s(
    filepath text,
    data_date date,
    filestatus text,
    file_proper text,
    link_nums text, 
    users text,
    groups text,
    size text,
    mtime date,
    filename text,
    check_date date
);""" % tablename

    @classmethod
    def init(cls, db_name='banks_arrive.db', schema_filename=init_sql):
        cls.db_name = db_name
        db_is_new = not os.path.exists(cls.db_name)
        with sqlite3.connect(cls.db_name) as conn:
            if db_is_new:
                logger.info('Creating schema')
                with open(schema_filename, 'r', encoding='utf8') as f:
                    schema = f.read()
                conn.executescript(schema)
            else:
                logger.info('Database {} exists!'.format(cls.db_name))

    @classmethod
    def _connect_db(cls):
        cls.init()
        cls.conn = sqlite3.connect(cls.db_name)
        logger.info("数据库名称：{}".format(cls.db_name))
        cursor = cls.conn.cursor()
        return cursor

    @classmethod
    def get_fetch(cls, sqltext, recatch='one'):
        """recatch： one or all
            返回单条数据或所有数据
            one类型：元组
            all类型：列表中嵌套元组
        """
        cursor = cls._connect_db()
        cursor.execute(sqltext)
        if recatch in ['one', 'all']:
            if recatch == 'one':
                row = cursor.fetchone()
            elif recatch == 'all':
                row = cursor.fetchall()
        else:
            raise ("{} 输入应该为 ['one', 'all']").format(recatch)
        cls.conn.commit()
        cls.conn.close()
        return row


class get_date():
    def __init__(self, filename, data_date=None):
        """
        返回需要检查文件的日期
        """
        if data_date:
            if not is_valid_date(data_date):
                raise "输入检查日期有误， 请检查:{}\n".format(data_date)

        self.data_date = data_date # 为None/传入的有效日期
        self.filename = filename

    def get_data_date(self):
        logger.info("需要检查的数据文件为： {}".format(self.filename))
        '''获取文件对应需要检查的日期'''
        if self.data_date:
            # 日期为输入
            pass
        else:
            try:
                # 读取sqlite3中数据文件最大日期， 再加一天
                max_date_filename_sql = '''select max(data_date) from {tablename} where filesname={filename} and filestatus=0;'''.format(tablename=tablename, filename=self.filename)
                print(max_date_filename_sql)
                (max_date,) = conn_sqlite.get_fetch(max_date_filename_sql)
                print(max_date)
            except:
                max_date = None
            if max_date:
                if isinstance(max_date, int):
                    max_date = str(max_date)
                    self.data_date = (datetime.datetime.strptime(max_date, '%Y%m%d') + datetime.timedelta(1)).strftime('%Y%m%d')
            else:
                self.data_date = (datetime.datetime.now() - datetime.timedelta(1)).strftime('%Y%m%d')
        logger.info("需要检查日期为： {}".format(self.data_date))
        return self.data_date

    def __call__(self):
        return self.get_data_date()


class files_check():
    def __init__(self, dirname, data_date):
        """
        对日期文件夹进行检查，并插入状态到sqlite中
        返回状态：
        '0', 正常
        '1', 文件正在写入
        '2', 文件完成，实际为空
        """
        self.dirname = dirname
        self.data_date = get_date(self.dirname, data_date)()
        self.check_dirname = dirname.format(date=self.data_date)

        logger.info("待检查文件为： {}".format(self.check_dirname))

    def check_hdfs(self):
        cmd = 'hadoop fs -ls -R "%s"' % self.check_dirname
        status, output = commands.getstatusoutput(cmd)
        if status:
            raise Exception("检查{}时出错: {}".format(self.check_dirname, output))

        patt = re.compile('\s+')
        output = output.split('\n')
        out_res = []
        filestatus = 0
        processing = False
        for line in output:
            line = patt.split(line)
            if len(line) > 5:
                file_proper, link_nums, users, groups, size, mtime_date, mtime_time, filename = line
                mtime = ' '.join([mtime_date, mtime_time])
                if '_SUCCESS' in filename:
                    continue
                if 'hive-staging' in filename:
                    processing = True
                if '_COPYING_' in filename:
                    processing = True
                if '_temporary' in filename:
                    processing = True

                if processing:
                    filestatus = '1'
                elif size < 100:
                    filestatus = '2'

                out_res.append(
                    [self.dirname, self.data_date, filestatus, file_proper, link_nums, users, groups, size, mtime,
                     filename])
        return out_res

    def out_check(self):


    def insert_sqlite(self):
        sqltext = '''insert into {tablename} values {out_res};'''.format(tablename=tablename, out_res=self.out_res)
        logger.info("插入语句为： {}".format(sqltext))
        conn_sqlite.get_fetch(sqltext)

    def __call__(self):
        self.insert_sqlite()
        out_res = list(self.out_res[:3])
        out_res.append(self.sqlite_table)
        return out_res


