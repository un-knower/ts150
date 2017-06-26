#!/usr/bin/env python
#coding:utf8

import os, sys, re


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
    sql_array = []
    for sql in all_lines.split(';'):
        if sql.strip() != '':
            sql_array.append(sql.strip())

    return sql_array


# 获取SQL脚本文件中的注释语句中包含k-v
def getRemarkKV(fileName, remark_flag=None):
    ret_kv_array = []
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
            if len(kv_array) <= 1 or kv_array[1].strip() == '':
                continue

            # 需要处理表达式中的等号
            express = '='.join(kv_array[1:]).strip().replace('"', '').replace("'", "")

            ret_kv_array.append( (kv_array[0], express) )

        f.close()

    return ret_kv_array


# 获取SQL脚本文件中变量
def getVar(fileName):
    var_list = []
    patt = re.compile(r'\$\{(\w+)\}', re.M)

    for line in getSql(fileName):
        # print '[%s]' % line

        var_list.extend(patt.findall(line))

    return set(var_list)

