#!/usr/bin/python
#coding:utf-8

import sys, os, re, time
import xlrd
sys.path.append("../python_common/")
import log
from common_fun import *
import pypage


class Excel():
    """docstring for excel"""
    def __init__(self):
        self.filename = r'..\violate\0_doc\DID31136_P9_SORBASE_物理模型_已上线.xlsx'.decode('utf-8').encode('gbk')
        self.table_list = [
                #N-RCC   收单
                # 线下批量交易流水表
                'T0291_BTH_TXN_JNL_A',
                # 线上批量交易流水表
                'T0291_BTH_ONL_TXN_JNL_A',

                #登录痕迹
                'T1000_LOGIN_TRAD_FLOW_A',
                #签约痕迹
                'T1000_SIGN_MAIN_FLOW_A',
                #帐户签约信息痕迹
                'T1000_SIGN_PACCT_FLOW_A',
                #渠道签约信息痕迹
                'T1000_SIGN_PCHANL_FLOW_A',
                #快捷支付签约流水
                'T1000_FASTPAY_SIGN_FL0_A',
                #快捷支付签约信息表
                'T1000_FASTPAY_SIGN_IN0_A',
                #个人网银常用收款账户
                'TODEB_EBS_ACCRECORD_H',
                #交易痕迹
                'T1000_TRAD_FLOW_A',
                #交易支付痕迹
                'T1000_PAY_TRAD_FLOW_A',
                #快捷支付交易痕迹
                'T1000_FASTPAY_TRAD_FL0_A',
                #超网交易痕迹
                'T1000_TXN_TRAD_FLOW_A',
                #龙支付交易记录表
                'T1000_LONGPAY_TRAD_FL0_A',

                #N-DPSP  对私存款
                #对私活期存款合约
                'T0182_TBSPACN0_H',
                #对私活期存款合约明细
                'T0182_TBSPTXN0_A',


                #ERM 电子银行风险监控子系统
                #挂起交易明细表
                'TODEM_TBL_SUSPEND_DET0_A',

                #N-ELB   电子银行
                #账号支付
                'T0431_ACCT_PAYMENT_A',
            ]


    def read_excel(self):
        data = xlrd.open_workbook(self.filename)
        for sheet in data.sheet_names():
            print sheet
        print data.sheet_names()

        table_sheet = data.sheet_by_name(u'表级')
        field_sheet = data.sheet_by_name(u'字段级')
        # table_sheet = data.sheet_by_index(3)
        # field_sheet = data.sheet_by_index(5)

        # 获取行数和列数
        # nrows = table_sheet.nrows
        # ncols = table_sheet.ncols

        # print nrows, ncols

        self.table_map = {}
        # 循环行,得到索引的列表
        for rownum in range(1, table_sheet.nrows):
            # print table_sheet.row_values(rownum)
            table_en = table_sheet.cell(rownum, 8).value
            table_cn = table_sheet.cell(rownum, 7).value

            table_en = table_en.upper()
            if table_en not in self.table_list:
                continue

            print table_en
            print table_cn

            self.table_map[table_en.upper()] = table_cn
            # if rownum > 10:
            #     break

        self.table_field_map = {}
        # 循环行,得到索引的列表
        for rownum in range(1, field_sheet.nrows):
            table_en = field_sheet.cell(rownum, 1).value
            table_en = table_en.upper()
            if table_en not in self.table_list:
                continue

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
                self.table_field_map[table_en] = field_array

            if field_en == 'P9_START_DATE':
                field_cn = u'P9开始日期'

            if field_en == 'P9_END_DATE':
                field_cn = u'P9结束日期'

            if field_cn == 'N/A':
                field_cn = ''

            field_cn = field_cn.replace('#', '').encode('utf-8')

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





if __name__ == '__main__':
    # main()
    xls = Excel()
    xls.read_excel()
