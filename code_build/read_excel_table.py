#!/usr/bin/python
#coding:utf-8

import sys, os, re, time
import xlrd
sys.path.append("../python_common/")
import log
from common_fun import *
import pypage


def read_excel():
    data = xlrd.open_workbook(r'..\violate\0_doc\SIAM_非本人交易_表结构_201612.xlsx'.decode('utf-8').encode('gbk'))
    # for sheet in data.sheet_names():
    #     print sheet
    #     print sheet.decode('utf-8')
    # print data.sheet_names()

    # table = data.sheet_by_name('案件溯源源表')
    table_sheet = data.sheet_by_index(4)
    field_sheet = data.sheet_by_index(5)

    # 获取行数和列数
    nrows = table_sheet.nrows
    ncols = table_sheet.ncols

    # print nrows, ncols
    return table_sheet, field_sheet


def read_table_name(table_sheet):
    table_map = {}
    # 循环行,得到索引的列表
    for rownum in range(1, table_sheet.nrows):
        # print table_sheet.row_values(rownum)
        table_en = table_sheet.cell(rownum,3).value
        table_cn = table_sheet.cell(rownum,4).value

        # print table_cn

        table_map[table_en.upper()] = table_cn.encode('utf-8')

    # for en, cn in table_map.items():
    #     print '%s,%s' % (en, cn)

    return table_map


def read_field_name(field_sheet):
    table_field_map = {}
    # 循环行,得到索引的列表
    for rownum in range(1, field_sheet.nrows):
        table_en = field_sheet.cell(rownum,1).value
        table_cn = field_sheet.cell(rownum,2).value
        field_en = field_sheet.cell(rownum,3).value
        field_cn = field_sheet.cell(rownum,4).value
        field_type = field_sheet.cell(rownum,5).value
        field_length = str(field_sheet.cell(rownum,6).value)
        field_is_pk = field_sheet.cell(rownum,7).value
        field_is_dk = field_sheet.cell(rownum,8).value
        field_to_ctbase = field_sheet.cell(rownum,10).value
        index_describe = field_sheet.cell(rownum,11).value
        index_function = field_sheet.cell(rownum,12).value
        index_split = field_sheet.cell(rownum,13).value
        field_filter = field_sheet.cell(rownum,14).value

        table_en = table_en.upper()
        field_en = field_en.upper()
        if table_en in table_field_map.keys():
            field_array = table_field_map[table_en]
        else:
            field_array = []
            table_field_map[table_en] = field_array

        if field_en == 'P9_START_DATE':
            field_cn = 'P9开始日期'

        if field_en == 'P9_END_DATE':
            field_cn = 'P9结束日期'

        if field_en in ('P9_START_BATCH', 'P9_END_BATCH', 'P9_DEL_FLAG', 'P9_JOB_NAME'):
            field_cn = ''

        if not isinstance(field_cn, int):
            # print field_cn
            field_cn = field_cn.strip().replace('#', '')  #.encode('utf-8')
        else:
            field_cn = ''

        if field_cn == 'N/A':
            field_cn = ''

        #数值类型数据，长度带,处理
        length_array = field_length.split(',')
        if len(length_array) > 1:
            field_length = length_array[0]

        field_array.append((field_en, field_cn, field_type, field_length, field_is_pk,
                            field_is_dk, field_to_ctbase, index_describe, index_function,
                            index_split, field_filter))

    return table_field_map


# 从文件或GP无数据获取表结构
def write_table_to_file(table_en, table_cn, field_array):
    template_file = './template/excel_{{table_en}}.py'
    var_map = {'table_en':table_en, 'table_cn':table_cn, 'field_array':field_array}

    path = './excel_table_field'
    mkdir(path)
    output_file = '%s/%s' % (path, os.path.split(pypage.pypage(template_file, var_map))[1])

    # 生成表字段描述Python代码
    pypage.pypage_from_file(template_file, var_map, output_file)


def main():
    table_sheet, field_sheet = read_excel()
    table_map = read_table_name(table_sheet)
    table_field_map = read_field_name(field_sheet)

    for table_en, table_cn in table_map.items():
        if table_en in table_field_map.keys():
            field_array = table_field_map[table_en]
            write_table_to_file(table_en, table_cn, field_array)


    table_list = [
        #对私客户信息
        'T0042_TBPC1010_H', 'T0042_TBPC9030_H', 'T0042_TBPC1510_H',
        #ECTIP
        'TODEC_TRAD_FLOW_A', 'TODEC_QUERY_TRAD_FLOW_A', 'TODEC_LOGIN_TRAD_FLOW_A',
        #CCBS ATM
        'TODDC_CRATMATM_SH', 'TODDC_CRATMDET_A',
        #CCBS POS
        'TODDC_CRPOSPOS_H', 'TODDC_CRDETDET_A',
        #CCBS 主档
        'TODDC_CRCRDCRD_H', 'TODDC_SAACNACN_H',
        #CCBS 明细流水
        'TODDC_SAACNTXN_A', 'TODDC_SAETXETX_A',
        #CCBS 柜员
        'TODDC_FCMTLR0_H',
        #机构表、机构关系、员工
        'T0651_CCBINS_INF_H', 'T0651_CCBINS_REL_H', 'T0861_EMPE_H',
        #信用卡
        'T0281_TBB1PLT0_H', 'T0281_S11T1_BILL_DTL_A'
    ]


if __name__ == '__main__':
    main()
    # a = ['1', '2', '3']
    # b = ['  %s' % x for x in a]

    # print ','.join(b)
