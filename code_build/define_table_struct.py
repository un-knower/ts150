#!/usr/bin/env python
#coding: utf-8

import sys, os, re, time

# fromExcel, fromGP = False, True
fromExcel, fromGP = True, False


#定义转换后对应的列
def define_change_field(field_array):

    change_field_array = []

    for field in field_array:
        (field_en, field_cn, field_type, field_length, field_is_pk, field_is_dk, field_to_ctbase, index_describe, index_function, index_split, field_filter) = field

        #风险监控字段需要拆分成三个字段
        if field_en == 'MON_INF':
            change_field_array.append(('TERM_QRY', u'登录设备查询', 'VARCHAR(48)', '48', 'N', 'N', 'Y', 'IDX_TERM_QRY', 'reverse(TERM_QRY,2)', '0,1,2,3,4,5,6,7,8,9,A,B,C,D,E,F,G,H,I,J,K,L,M,N,O,P,Q,R,S,T,U,V,W,X,Y,Z', 'y'))
            change_field_array.append(('TERM_BIOS', u'PC机BIOS序列号', 'VARCHAR(9)', '9', 'N', 'N', 'Y', '', '', '', 'y'))
            change_field_array.append(('TERM_IMEI', u'手机标识', 'VARCHAR(48)', '48', 'N', 'N', 'Y', '', '', '', 'y'))
            change_field_array.append(('TERM_MAC', u'网卡Mac地址', 'VARCHAR(40)', '40', 'N', 'N', 'Y', '', '', '', 'y'))
        elif field_en == 'TERM_INF':
            change_field_array.append(('MOBILE', u'手机号', 'VARCHAR(16)', '16', 'N', 'N', 'Y', 'IDX_MOBILE', 'reverse(MOBILE,2)', '0,1,2,3,4,5,6,7,8,9', 'y'))
            change_field_array.append(('IP', u'IP地址', 'VARCHAR(32)', '32', 'N', 'N', 'Y', 'IDX_IP', 'reverse(IP,2)', '0,1,2,3,4,5,6,7,8,9', 'y'))
        else:
            if field_en not in ('P9_START_BATCH', 'P9_END_BATCH', 'P9_DEL_FLAG', 'P9_JOB_NAME', 'P9_BATCH_NUMBER', 'P9_DEL_DATE', 'P9_DEL_BATCH', 'P9_SPLIT_BRANCH_CD'):
                change_field_array.append(field)

    return change_field_array


#定义目标贴源表对应的列
def define_sor_field(change_field_array):
    sor_field_array = []
    partition_field = ""

    for field in change_field_array:
        (field_en, field_cn, field_type, field_length, field_is_pk, field_is_dk, field_to_ctbase, index_describe, index_function, index_split, field_filter) = field

        # 分区字段不放入字段列表中
        if field_is_dk == "Y":
            partition_field = field_en

        if field_is_pk == "Y":
            pk_field = field_en

        # else:
        sor_field_array.append(field)

    return (sor_field_array, partition_field, pk_field)


#定义目标贴源表对应的列
def define_ctbase_field(change_field_array):
    ctbase_field_array = []
    partition_field = ""

    for field in change_field_array:
        (field_en, field_cn, field_type, field_length, field_is_pk, field_is_dk, field_to_ctbase, index_describe, index_function, index_split, field_filter) = field

        if field_to_ctbase == 'Y':
            ctbase_field_array.append(field)

            if field_is_dk == "Y":
                partition_field = field_en

    return (ctbase_field_array, partition_field)


#定义目标贴源表对应的列
def define_filter_field(change_field_array):
    field_array = []
    partition_field = ""

    for field in change_field_array:
        (field_en, field_cn, field_type, field_length, field_is_pk, field_is_dk, field_to_ctbase, index_describe, index_function, index_split, field_filter) = field

        if field_filter != '':
            field_array.append(field)

        if field_is_dk == "Y":
            partition_field = field_en

    return (field_array, partition_field)


class TableDefine():
    """表定义"""
    def __init__(self, table_en, table_cn, field_array):
        self.table_en = table_en
        self.table_cn = table_cn
        self.field_array = field_array
        self.change_field_array = define_change_field(self.field_array)
        self.sor_field_array, partition_field, self.pk_field = define_sor_field(self.change_field_array)
        self.ctbase_field_array, partition_field = define_ctbase_field(self.change_field_array)
        # self.partition_field = partition_field
        self.filter_field_array, partition_field = define_filter_field(self.change_field_array)
        self.partition_field = partition_field


class SlideTableStruct():
    """_H 静态信息表 拉链表"""
    def __init__(self, table_list=None):

        if table_list:
            self.table_list = table_list
        else:
            self.table_list = [
                # #对私客户信息
                # 'T0042_TBPC1010_H', 'T0042_TBPC9030_H', 'T0042_TBPC1510_H',
                # #CCBS 主档
                # 'TODDC_CRCRDCRD_H', 'TODDC_SAACNACN_H',
                # #CCBS 柜员
                # 'TODDC_FCMTLR0_H',
                # #机构表、机构关系、员工
                # 'T0651_CCBINS_INF_H', 'T0651_CCBINS_REL_H', 'T0861_EMPE_H'
                'T0281_S11T1_BILL_DTL_A',
                'T0281_TBB1PLT0_H'
            ]

        self.exist_table_map = {}
        if fromExcel:
            import read_excel_table
            table_sheet, field_sheet = read_excel_table.read_excel()
            self.table_map = read_excel_table.read_table_name(table_sheet)
            self.table_field_map = read_excel_table.read_field_name(field_sheet)

            for table_en in self.table_list:
                td = TableDefine(table_en, self.table_map.get(table_en, ''), self.table_field_map[table_en])
                self.exist_table_map[table_en] = td

        if fromGP:
            import read_gp_table
            for table_en in self.table_list:
                table_cn, field_array = read_gp_table.get_gp_table('base_old.%s' % table_en)
                td = TableDefine(table_en, table_cn, field_array)
                self.exist_table_map[table_en] = td


    def __getattr__(self, table_en):
        # print 'attr=%s' % table_name
        # print dir(self)
        if table_en in self.table_list:
            return self.exist_table_map[table_en]
        else:
            raise "表名未定义"


def main():
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
        'T0281_TBB1PLT0_H', 'T0281_S11T1_BILL_DTL_A', 'T0281_S11T1_BIL_DSP_D0_A'
        ]


    sts = SlideTableStruct(table_list)
    print sts.TODDC_SAACNACN_H.table_cn
    print sts.TODDC_SAACNACN_H.table_en
    print sts.TODDC_SAACNACN_H.sor_field_array
    print sts.TODDC_SAACNACN_H.ctbase_field_array

    # mts = MiddleTableStruct()
    # for table_en, td in mts.exist_table_map.items():
    #     print '-------%s-----' % table_en
    #     for field in td.ctbase_field_array:
    #         print field[0], field[1]

    # print mds.MID_CUSTOMER_CUR.ctbase_field_array

    # for table_en in table_list:
    #     table_en = table_en.upper()
    #     table_cn = table_map[table_en]
    #     field_array = table_field_map[table_en]

    # build_makefile(table_list)


if __name__ == '__main__':
    main()
    # a = ['1', '2', '3']
    # b = ['  %s' % x for x in a]

    # print ','.join(b)
