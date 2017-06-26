#!/usr/bin/env python
#coding: utf-8

import sys, os, re, time
from define_table_struct import *
sys.path.append("../python_common/")
import pypage
from common_fun import *


#生成建Hive表的SQL脚本文件
def build_hive_create_sql(td, template_file):
    ext_field_list = [(field[0], field[1]) for field in td.field_array]
    sor_field_list = [(field[0], field[1]) for field in td.sor_field_array]
    ctbase_field_list = [(field[0], field[1]) for field in td.ctbase_field_array
                         if field[0] != 'P9_DATA_DATE']
    filter_field_list = [(field[0], field[1]) for field in td.filter_field_array]
    var_map = {'table_en':td.table_en, 'table_cn':td.table_cn,
               'ext_field_list':ext_field_list,
               'sor_field_list':sor_field_list,
               'ctbase_field_list':ctbase_field_list,
               'filter_field_list':filter_field_list,
               'partition_field':td.partition_field}


    output_file = './hive_create/%s' % os.path.split(pypage.pypage(template_file, var_map))[1]
    # print '[%s][%s]' % (template_file, output_file)

    pypage.pypage_from_file(template_file, var_map, output_file)


#生成插入Hive表的SQL脚本文件
def build_hive_insert_sql(td, template_file):
    ext_field_list = [(field[0], field[1]) for field in td.field_array]
    sor_field_list = [(field[0], field[1]) if field[2] != 'DATE'
                      else ("regexp_replace(%s, '-', '')" % field[0], field[1])
                      for field in td.sor_field_array]
    ctbase_field_list = [(field[0], field[1]) for field in td.ctbase_field_array]
    ctbase_change_field_list = [(field[0], field[1]) if field[2] != 'DATE'
                                else ("regexp_replace(%s, '-', '')" % field[0], field[1])
                                for field in td.ctbase_field_array
                                if field[0] != 'P9_DATA_DATE']

    filter_field_list = [(field[0], field[1]) for field in td.filter_field_array]

    var_map = {'table_en':td.table_en, 'table_cn':td.table_cn,
               'ext_field_list':ext_field_list,
               'sor_field_list':sor_field_list,
               'ctbase_field_list':ctbase_field_list,
               'ctbase_change_field_list':ctbase_change_field_list,
               'filter_field_list':filter_field_list,
               'partition_field':td.partition_field}

    output_file = './hive_insert/%s' % os.path.split(pypage.pypage(template_file, var_map))[1]

    pypage.pypage_from_file(template_file, var_map, output_file)


#生成插入Hive表的SQL脚本文件
def build_mid_table_hive_insert_sql(td, sub_td_list, template_file):
    ctbase_field_list = [(field[0], field[1]) for field in td.ctbase_field_array]
    filter_field_list = [(field[0], field[1]) for field in td.filter_field_array]

    assert(len(sub_td_list) == 2)

    sub_td = sub_td_list[0]
    if sub_td.table_en[-2:] == '_H':
        master_table_en = 'CT_' + sub_td.table_en
        master_where = "P9_END_DATE = '29991231'"
    else:
        master_table_en = 'INN_' + sub_td.table_en
        master_where = "P9_DATA_DATE='${log_date}'"

    master_table_cn = sub_td.table_cn
    master_pk_field = sub_td.pk_field
    master_ctbase_field_list = [(field[0], field[1]) for field in sub_td.ctbase_field_array
                                if field[0] not in ('P9_START_DATE', 'P9_END_DATE')]
    master_filter_field_list = [(field[0], field[1]) for field in sub_td.filter_field_array
                                if field[0] not in ('P9_START_DATE', 'P9_END_DATE')]

    sub_td = sub_td_list[1]
    if sub_td.table_en[-2:] == '_H':
        slave_table_en = 'CT_' + sub_td.table_en
        slave_where = "P9_END_DATE = '29991231'"
    else:
        slave_table_en = 'INN_' + sub_td.table_en
        slave_where = "P9_DATA_DATE='${log_date}'"
    slave_table_cn = sub_td.table_cn
    slave_pk_field = sub_td.pk_field
    slave_ctbase_field_list = [(field[0], field[1]) for field in sub_td.ctbase_field_array
                               if field[0] not in ('P9_START_DATE', 'P9_END_DATE')]
    slave_filter_field_list = [(field[0], field[1]) for field in sub_td.filter_field_array
                               if field[0] not in ('P9_START_DATE', 'P9_END_DATE')]

    var_map = {'table_en':td.table_en, 'table_cn':td.table_cn,
               'ctbase_field_list':ctbase_field_list,
               'filter_field_list':filter_field_list,
               'partition_field':td.partition_field,

               'master_table_en':master_table_en,
               'master_table_cn':master_table_cn,
               'master_ctbase_field_list':master_ctbase_field_list,
               'master_filter_field_list':master_filter_field_list,
               'master_where':master_where,
               'master_pk_field':master_pk_field,

               'slave_table_en':slave_table_en,
               'slave_table_cn':slave_table_cn,
               'slave_ctbase_field_list':slave_ctbase_field_list,
               'slave_filter_field_list':slave_filter_field_list,
               'slave_where':slave_where,
               'slave_pk_field':slave_pk_field,
               }

    output_file = './hive_insert/%s' % os.path.split(pypage.pypage(template_file, var_map))[1]

    pypage.pypage_from_file(template_file, var_map, output_file)


#生成脚本文件
def build_script(td, template_file, savepath):
    ext_field_list = [(field[0], field[1]) for field in td.field_array]
    sor_field_list = [(field[0], field[1]) if field[2] != 'DATE'
                      else ("regexp_replace(%s, '-', '')" % field[0], field[1])
                      for field in td.sor_field_array]
    ctbase_field_list = [(field[0], field[1]) for field in td.ctbase_field_array]
    filter_field_list = [(field[0], field[1]) for field in td.filter_field_array]

    var_map = {'table_en':td.table_en, 'table_cn':td.table_cn,
               'ext_field_list':ext_field_list,
               'sor_field_list':sor_field_list,
               'ctbase_field_list':ctbase_field_list,
               'filter_field_list':filter_field_list,
               'partition_field':td.partition_field}

    output_file = '%s/%s' % (savepath, os.path.split(pypage.pypage(template_file, var_map))[1])
    # print '[%s][%s]' % (template_file, output_file)

    pypage.pypage_from_file(template_file, var_map, output_file)


# 流水表
def flow_table():
    table_list = [
        #CCBS 明细流水
        # 'TODDC_SAACNTXN_A',
        # 'TODDC_SAETXETX_A',
        'T0281_S11T1_BILL_DTL_A',
    ]
    sts = SlideTableStruct(table_list)
    # build_hive_create_sql(sts.TODDC_CRCRDCRD_H)
    for table_en, td in sts.exist_table_map.items():
        build_hive_create_sql(td, './template/CREATE_FLOW_{{table_en}}.sql')
        build_hive_insert_sql(td, './template/INSERT_FLOW_{{table_en}}.sql')
        build_script(td, './template/EXPORT_GP_FLOW_{{table_en}}.sh', './2_ready_data')
        build_script(td, './template/UPLOAD_{{table_en}}.sh', './2_ready_data')
        print 'table: %s finish' % table_en


# 拉链表
def slide_table():
    table_list = [
        #信用卡
        'T0281_TBB1PLT0_H',
    ]
    sts = SlideTableStruct(table_list)
    # build_hive_create_sql(sts.TODDC_CRCRDCRD_H)
    for table_en, td in sts.exist_table_map.items():
        build_hive_create_sql(td, './template/CREATE_SLIDE_{{table_en}}.sql')
        build_hive_insert_sql(td, './template/INSERT_SLIDE_{{table_en}}.sql')
        build_script(td, './template/EXPORT_GP_SLIDE_{{table_en}}.sh', './2_ready_data')
        build_script(td, './template/UPLOAD_{{table_en}}.sh', './2_ready_data')
        print 'table: %s finish' % table_en


# 全量小表
def full_table():
    table_list = [
        #CCBS 明细流水
        'TODDC_SAACNTXN_A',
        'TODDC_SAETXETX_A',
    ]
    template_file = './template/CREATE_FLOW_{{table_en}}.sql'
    sts = SlideTableStruct(table_list)
    # build_hive_create_sql(sts.TODDC_CRCRDCRD_H)
    for table_en, td in sts.exist_table_map.items():
        # build_hive_create_sql(td)
        build_hive_insert_sql(td)
        print 'table: %s finish' % table_en


# 中间表
def middle_table():

    table_join_map = {
        #对私客户信息
        # 'MID_CUSTOMER_CUR':(u'对私客户信息整合表，带原系统客户号', ('T0042_TBPC9030_H', 'T0042_TBPC1010_H')),
        #卡与帐户中间表
        # 'MID_CARD_CUR':(u'CCBS帐户与卡整合表', ('TODDC_SAACNACN_H','TODDC_CRCRDCRD_H')),
        #客户与卡中间表
        # 'MID_CARD_CUSTOMER_CUR':(u'CCBS账户与客户信息整合表', ('MID_CARD_CUR', 'MID_CUSTOMER_CUR')),
        #机构表、机构关系、员工
        # 'MID_CCBINS_CUR':(u'机构表', ('T0651_CCBINS_INF_H', 'T0651_CCBINS_REL_H')),
        # 'MID_EMPE_CCBINS_CUR':(u'员工机构信息表', ('T0861_EMPE_H', 'MID_CCBINS_CUR')),
        # 'MID_FCMTLR0_CCBINS_CUR':(u'柜员机构信息表', ('TODDC_FCMTLR0_H', 'MID_CCBINS_CUR')),

        # 信用卡
        'MID_CREDIT_BILL_DTL_A':('信用卡账单表', ('T0281_S11T1_BILL_DTL_A', 'T0281_TBB1PLT0_H')),
    }


    # 从贴源表生成的中间表
    table_list = []
    for target_table, (table_cn, src_join_set) in table_join_map.items():
        all_field_list = []
        for src_table in src_join_set:
            # 收集所有贴源表
            if src_table[:4] != 'MID_':
                table_list.append(src_table)

    # 读取贴源表
    sts = SlideTableStruct(table_list)

    # 两次循环，构造中间结构
    all_table_map = {}
    mid_table_map = {}
    for i in range(2):
        for target_table, (table_cn, src_join_set) in table_join_map.items():
            all_field_list = []
            for src_table in src_join_set:
                # 源表为贴源数据
                if src_table in sts.exist_table_map.keys():
                    td = sts.exist_table_map[src_table]
                    all_table_map[src_table] = td
                    all_field_list.extend(td.field_array)
                else:
                    # 源表为中间表
                    if src_table in all_table_map.keys():
                        td = all_table_map[src_table]
                        all_field_list.extend(td.field_array)
                    else:
                        break
            else:
                # 排除不要的字段与重复的字段
                filter_field_list = []
                for field in all_field_list:
                    field_en = field[0]
                    if field_en in ('P9_START_DATE', 'P9_END_DATE'):
                        continue
                    if field_en in [x[0] for x in filter_field_list]:
                        continue

                    filter_field_list.append(field)

                target_td = TableDefine(target_table, table_cn, filter_field_list)
                all_table_map[target_table] = target_td
                mid_table_map[target_table] = target_td

    # print all_table_map
    for table_en, td in mid_table_map.items():
        build_hive_create_sql(td, './template/CREATE_{{table_en}}.sql')

        sub_td_list = []
        table_cn, src_join_set = table_join_map[table_en]
        for src_table in src_join_set:
            sub_td = all_table_map[src_table]
            sub_td_list.append(sub_td)

        build_mid_table_hive_insert_sql(td, sub_td_list, './template/INSERT_{{table_en}}.sql')

        print 'table: %s finish' % table_en


def main():
    #目录不存在，则新建
    mkdir('./hive_create')
    mkdir('./hive_insert')
    mkdir('./2_ready_data')

    # slide_table()
    # flow_table()
    middle_table()


if __name__ == '__main__':
    main()
