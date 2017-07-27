#!/usr/bin/env python
#coding:utf8

import sys
from dbHelper import *
from var import *
from bottle import route, run, request
sys.path.append("../python_common/")
import log
from common_fun import *


dbHelper = DbHelper()


@route('/hello', method='POST')
@route('/hello')
def hello():
    return "Hello World!"


@route('/test', method='POST')
def test():
    print request.forms.get('sql')
    print request.forms.get('age')
    return str(dbHelper.query_sql(sql))


@route('/sql/select', method='POST')
def select():
    sql = request.forms.get('sql')
    row_array = dbHelper.query_sql(sql)

    return str(row_array)


@route('/sql/insert', method='POST')
def insert():
    sql = request.forms.get('sql')
    log.debug(sql)
    dbHelper.cur.execute(sql)
    return str(dbHelper.cur.lastrowid)


@route('/sql/delete', method='POST')
def delete():
    sql = request.forms.get('sql')
    log.debug(sql)
    dbHelper.cur.execute(sql)
    return str(dbHelper.cur.rowcount)


@route('/sql/update', method='POST')
def update():
    sql = request.forms.get('sql')
    log.debug(sql)
    dbHelper.cur.execute(sql)
    return str(dbHelper.cur.rowcount)


def main():
    run(host='0.0.0.0', port=http_port)


if __name__ == '__main__':
    main()
