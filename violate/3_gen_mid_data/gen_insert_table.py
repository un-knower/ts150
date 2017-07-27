#!/usr/bin/python
#coding:gbk

import sys, os, re, time
from gen_create_table import *

sys.path.append("../1_create_table/")
from read_data_struct import *

script_path = "/home/ap/dip/appjob/shelljob/TS150/violate"


#生成中间表插入的SQL脚本文件，通过Join连接
def build_hive_middle_insert_sql(mts, td):
    (table_cn, src_join_set) = mts.table_join_map[td.table_en]
    # return 0

    target_field_stmt_list = ["   -- %s\n   %s" % (field[1], field[0]) 
                              for field in td.ctbase_field_array 
                              if field[0] not in ('P9_START_DATE', 'P9_END_DATE')]
    target_field_stmt = ",\n".join(target_field_stmt_list)

    sql = u'''use sor;

-- 中间表插入
-- %(cn)s: %(en)s

-- 有关联的数据插入
INSERT OVERWRITE TABLE %(en)s
SELECT
%(cols)s FROM
''' % {'en':td.table_en, 'cn':td.table_cn, 'cols':target_field_stmt}

    # 生成 链接源表字段插入语句
    sub_query_list = []
    for src_table in src_join_set:
        src_td = mts.exist_table_map[src_table]
        src_field_stmt_list = ["        -- %s\n        %s" % (field[1], field[0]) 
                               for field in src_td.ctbase_field_array
                               if field[0] not in ('P9_START_DATE', 'P9_END_DATE')]
        src_field_stmt = ",\n".join(src_field_stmt_list)
        pk = "\n    AND ".join(["(%s is not null AND %s <> '')" % (field[0], field[0])
                               for field in src_td.ctbase_field_array if field[4] == 'Y'])

        sub_sql = u'''(SELECT
%(cols)s
   FROM CT_%(en)s
  WHERE P9_END_DATE = '29991231'
    AND %(pk)s) t
''' % {'en':src_td.table_en, 'cols':src_field_stmt, 'pk':pk}

        sub_query_list.append(sub_sql)

    sql += 'INNER JOIN\n'.join(sub_query_list)

    # 主表字段
    master_td = mts.exist_table_map[src_join_set[0]]
    master_field_stmt_list = ["        %s" % (field[0]) 
                              for field in master_td.ctbase_field_array
                              if field[0] not in ('P9_START_DATE', 'P9_END_DATE')]
    pk = "\n   AND ".join(["(%s is null OR %s = '')" % (field[0], field[0])
                           for field in master_td.ctbase_field_array if field[4] == 'Y'])

    # 从表字段
    for src_table in src_join_set[1:]:
        src_td = mts.exist_table_map[src_table]
        src_field_stmt_list = ["        null as %s" % (field[0]) 
                               for field in src_td.ctbase_field_array
                               if field[0] not in ('P9_START_DATE', 'P9_END_DATE')]
        master_field_stmt_list.extend(src_field_stmt_list)

    master_field_stmt = ",\n".join(master_field_stmt_list)

    sql += u'''
-- 追加主表为空的记录
INSERT INTO TABLE %(en)s
SELECT
%(cols)s 
  FROM CT_%(src)s
 WHERE P9_END_DATE = '29991231'
   AND %(pk)s;
''' % {'en':td.table_en, 'cols':master_field_stmt, 'src':master_td.table_en, 'pk':pk}

    f = open(r'./hive_insert/INSERT_%s.sql' % td.table_en, 'w')
    f.write(sql.encode('utf-8'))
    f.close()

    shell=u'''#!/bin/sh
######################################################
#   %(cn)s: %(en)s中间表整合处理
#                   wuzhaohui@tienon.com
######################################################

#引用基础Shell函数库
source %(path)s/base.sh

#登录Hadoop
hadoop_login

#解释命令行参数
logdate_arg $*

# 依赖数据源--当天数据
IN_CUR_HIVE="%(src)s"

# Hive输出表，判断脚本是否已成功运行完成
OUT_CUR_HIVE="%(en)s"

run()
{
   beeline -f $script_path/3_gen_mid_data/hive_insert/INSERT_%(en)s.sql --hivevar log_date=${log_date}
}
''' % {'en':td.table_en, 'cn':td.table_cn, 'sql':sql, 'src':' '.join(src_join_set), 'path':script_path}

    f = open(r'./hive_insert/INSERT_%s.sh' % td.table_en, 'w')
    f.write(shell.encode('utf-8'))
    f.close()


def main():
    if not os.path.exists('./hive_insert'):  #目录不存在，则新建
        os.mkdir('./hive_insert')

    mts = MiddleTableStruct()
    # build_hive_middle_insert_sql(mts, mts.MID_CUSTOMER_CUR)
    for table_en, td in mts.exist_table_map.items():
        if table_en[0:4] == 'MID_':
            build_hive_middle_insert_sql(mts, td)
            print 'table: %s finish' % table_en


if __name__ == '__main__':
    main()
