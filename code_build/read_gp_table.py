#!/usr/bin/env python
#coding:utf8

import os, sys, re
import datetime
import pyetc
sys.path.append("../python_common/")
import log
from common_fun import *
import pypage


def split_table(table):
    # 表名分解，schema.table
    table_array = table.lower().split('.')
    assert len(table_array) == 2

    schema = table_array[0]
    table_name = table_array[1]

    return schema, table_name


# 读取GP表结构
def read_gp_table(table):
    schema, table_name = split_table(table)

    sql = "select c.attname, d.typname from pg_class a, pg_namespace b, pg_attribute c, pg_type d " + \
          " where a.relnamespace = b.oid and b.nspname = '%s' and a.relname = '%s'" % (schema, table_name) + \
          "   and a.oid = c.attrelid and c.atttypid = d.oid and attnum > 0" + \
          " order by attnum;"
    field_array = psql(schema, sql, logFile='create_gp_table')

    ret_field_array = []
    for field_map in field_array:
        field_type = field_map['typname']
        field_name = field_map['attname']

        # print '%s  %s,' % (field_name, field_type)
        field_en = field_name.upper()
        field_cn = ''
        field_type = field_type.upper()
        field_length = ''
        field_is_pk = ''
        field_is_dk = ''
        if field_en in ('P9_END_DATE', 'P9_DATA_DATE'):
            field_is_dk = 'Y'
        index_describe, index_function, index_split = '', '', ''
        field_to_ctbase, field_filter = 'Y', 'Y'
        if field_en in ('P9_START_BATCH', 'P9_END_BATCH', 'P9_DEL_FLAG', 'P9_JOB_NAME', 'P9_BATCH_NUMBER', 'P9_DEL_DATE', 'P9_DEL_BATCH', 'P9_SPLIT_BRANCH_CD'):
            field_to_ctbase, field_filter = '', ''
        ret_field_array.append((field_en, field_cn, field_type, field_length, field_is_pk, field_is_dk,
                                field_to_ctbase, index_describe, index_function, index_split, field_filter))
    # print ret_field_array

    return ret_field_array


# 从文件或GP无数据获取表结构
def get_gp_table(table, force=False):
    schema, table_name = split_table(table)
    table_cn = ''
    table_en = table_name.upper()

    template_file = './template/{{table_en}}.py'
    var_map = {'table_en':table_en, 'table_cn':''}

    path = './table_field'
    output_file = '%s/%s' % (path, os.path.split(pypage.pypage(template_file, var_map))[1])
    if not os.path.exists(output_file) or force:
        # 生成表字段描述Python代码
        field_array = read_gp_table(table)
        mkdir(path)
        table_en, table_cn, ret_field_array = renew_from_excel(table_name, field_array)
        var_map['table_cn'] = table_cn
        var_map['field_array'] = ret_field_array
        pypage.pypage_from_file(template_file, var_map, output_file)
    else:
        # 动态加载，执行修改后的Python代码
        table_module = pyetc.load(output_file)
        field_array = table_module.field_array
        table_cn = table_module.table_cn

    return table_cn, field_array


# 补充Excel文件中的表结构定义信息
def renew_from_excel(table_name, field_array):
    table_en = table_name.upper()
    excel_table_file = './excel_table_field/excel_%s.py' % table_en
    if not os.path.exists(excel_table_file):
        return table_en, '', field_array

    # 动态加载，读取Excel生成对应表的Python代码
    table_module = pyetc.load(excel_table_file)
    excel_field_array = table_module.field_array
    table_cn = table_module.table_cn
    ret_field_array = []

    for i in range(len(field_array)):
        field_en = field_array[i][0]

        for excel_field in excel_field_array:
            if field_en == excel_field[0]:
                field_cn = excel_field[1]
                field_to_ctbase = excel_field[2]
                field_filter = excel_field[3]
                field = (field_en, field_cn, field_array[i][2], field_array[i][3],
                         field_array[i][4], field_array[i][5],
                         field_to_ctbase, field_array[i][7], field_array[i][8],
                         field_array[i][9], field_filter)
                ret_field_array.append(field)
                break
        else:
            ret_field_array.append(field_array[i])

    return table_en, table_cn, ret_field_array


# GP SQL 执行
def psql(schema, sql, logFile='check_gp'):
    # 判断生产环境或测试环境
    if isLinux():
        ip_list = get_ip()
        for ip in ip_list:
            ip_num_array = ip.split('.')
            # IP以128开头，测试环境，其它为生产环境
            assert ip_num_array[0] in ('11', '128')
            if ip_num_array[0] == '128':
                is_test = True
            else:
                is_test = False
    else:
        is_test = True

    if schema in ('base', 'base_pro', 'aca_xk_view', 'base_old'):
        if is_test:
            cmd = 'psql -x -h 128.196.116.69 -d sordb_pl3 -U aca_xk_user -c "%s"'
        else:
            cmd = 'export HOME=/home/ap/dip;source $HOME/.bash_profile;export PGPASSWORD=password; psql -x -h 11.36.156.168 -d sordb -U aca_xk_user -c "%s"'
    else:
        if is_test:
            cmd = 'psql -x -h 128.196.116.70 -d acadb_pl2 -U aca_siam_etl -c "%s"'
        else:
            cmd = 'export HOME=/home/ap/dip;source $HOME/.bash_profile;psql -x -h 11.58.112.141 -d saldb -U aca_siam_etl -c "%s"'

    log.info(sql)
    retcode, outlines = executeShell(cmd % sql)
    if retcode != 0:
        log.error('GP命令执行失败:[%s][%d]' % (cmd % sql, retcode), logFile)
        return []

    out_array = []
    for line in outlines:
        if len(line) == 0: continue

        # print '[%s]' % line
        line_array = line.split('|')

        if len(line_array) == 1:
            if 'RECORD' in line:
                out_dict = {}
                out_array.append(out_dict)
            continue

        line_array = [x.strip() for x in line_array]

        out_dict[line_array[0]] = line_array[1]

    # log.debug(out_array)
    return out_array


def main():
    print get_gp_table('app_siam.toddc_Saacnacn_H')


if __name__ == '__main__':
    main()
