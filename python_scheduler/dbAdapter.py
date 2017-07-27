#!/usr/bin/env python
#coding:utf8

import sys
import httplib, urllib
from var import *
from dbHelper import *
sys.path.append("../python_common/")
import log


# Adapter
class DbAdapter:
    """数据库操作，本地与远程适配器"""
    def __init__(self, remote_mode=True):
        self.remote_mode = remote_mode

        if self.remote_mode:
            httpClient('/hello', '')
        else:
            self.dbHelper = DbHelper()

    def __del__(self):
        pass


    # 插入
    def insert(self, sql):
        if self.remote_mode:
            ret = httpClient('insert', sql)
            lastrowid = int(ret)
        else:
            self.dbHelper.cur.execute(sql)
            lastrowid = self.dbHelper.cur.lastrowid

        return lastrowid

    # 查询
    def select(self, sql):
        row_array = []
        if self.remote_mode:
            ret = httpClient('select', sql)
            if ret and ret <> '':
                row_array = eval(ret)
        else:
            row_array = self.dbHelper.query_sql(sql)

        return row_array


    def update(self, sql):
        if self.remote_mode:
            ret = httpClient('update', sql)
            rowcount = int(ret)
        else:
            self.dbHelper.cur.execute(sql)
            rowcount = self.dbHelper.cur.rowcount

        return rowcount


    def delete(self, sql):
        if self.remote_mode:
            ret = httpClient('delete', sql)
            rowcount = int(ret)
        else:
            self.dbHelper.cur.execute(sql)
            rowcount = self.dbHelper.cur.rowcount

        return rowcount


def httpClient(operate, sql):
    client = None
    try:
        params = urllib.urlencode({'sql':sql})
        headers = {"Content-type": "application/x-www-form-urlencoded",
                   "Accept": "text/plain"}
        if operate[0] == '/':
            url = operate
        else:
            url = "/sql/%s" % operate

        client = httplib.HTTPConnection(http_ip, http_port, timeout=30)
        client.request("POST", url, params, headers)

        response = client.getresponse()
        httpBody = response.read()
        if response.status == 200:
            return httpBody
        else:
            log.error(sql, 'httpSql')
            log.error(response.reason, 'httpSql')
            log.error(httpBody, 'httpSql')
            raise CommonError(msg='http请求sql执行:%s\n%s\n%s' % (sql, response.reason, httpBody))
        # print response.getheaders() #获取头信息
    except Exception, e:
        print e
    finally:
        if client:
            client.close()


def main():
    db = DbAdapter(remote_mode=True)
    db.select('select * from work_config limit 1')
    print 'Test Success'


if __name__ == '__main__':
    main()
