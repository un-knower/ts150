#!/usr/bin/python
#coding:gbk

import sys, os, re, time
import xlrd


def read_excel():
    data = xlrd.open_workbook(r'..\0_doc\SIAM_�Ǳ��˽���_��ṹ_201612.xlsx') 
    # for sheet in data.sheet_names():
    #     print sheet
    #     print sheet.decode('utf-8')
    # print data.sheet_names()

    # table = data.sheet_by_name('������ԴԴ��')
    table_sheet = data.sheet_by_index(4)
    field_sheet = data.sheet_by_index(5)

    # ��ȡ����������
    nrows = table_sheet.nrows
    ncols = table_sheet.ncols

    # print nrows, ncols
    return table_sheet, field_sheet


def read_table_name(table_sheet):
    table_map = {}
    # ѭ����,�õ��������б�
    for rownum in range(1, table_sheet.nrows):
        # print table_sheet.row_values(rownum)
        table_en = table_sheet.cell(rownum,3).value
        table_cn = table_sheet.cell(rownum,4).value

        table_map[table_en.upper()] = table_cn

    # for en, cn in table_map.items():
    #     print '%s,%s' % (en, cn)

    return table_map


def read_field_name(field_sheet):
    table_map = {}
    # ѭ����,�õ��������б�
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

        table_en = table_en.upper()
        field_en = field_en.upper()
        if table_en in table_map.keys():
            field_array = table_map[table_en]
        else:
            field_array = []
            table_map[table_en] = field_array

        if field_en == 'P9_START_DATE':
            field_cn = u'P9��ʼ����'

        if field_en == 'P9_END_DATE':
            field_cn = u'P9��������'

        if field_cn == 'N/A':
            field_cn = field_en

        #��ֵ�������ݣ����ȴ�,����
        length_array = field_length.split(',')
        if len(length_array) > 1:
            field_length = length_array[0]

        field_array.append((field_en, field_cn, field_type, field_length, field_is_pk, field_is_dk, field_to_ctbase, index_describe, index_function, index_split))

    return table_map


#����ת�����Ӧ����
def define_change_field(field_array):

    change_field_array = []

    for field in field_array:
        (field_en, field_cn, field_type, field_length, field_is_pk, field_is_dk, field_to_ctbase, index_describe, index_function, index_split) = field

        #���ռ���ֶ���Ҫ��ֳ������ֶ�
        if field_en == 'MON_INF':
            change_field_array.append(('TERM_QRY', u'��¼�豸��ѯ', 'VARCHAR(48)', '48', 'N', 'N', 'Y', 'IDX_TERM_QRY', 'reverse(TERM_QRY,2)', '0,1,2,3,4,5,6,7,8,9,A,B,C,D,E,F,G,H,I,J,K,L,M,N,O,P,Q,R,S,T,U,V,W,X,Y,Z'))
            change_field_array.append(('TERM_BIOS', u'PC��BIOS���к�', 'VARCHAR(9)', '9', 'N', 'N', 'Y', '', '', ''))
            change_field_array.append(('TERM_IMEI', u'�ֻ���ʶ', 'VARCHAR(48)', '48', 'N', 'N', 'Y', '', '', ''))
            change_field_array.append(('TERM_MAC', u'����Mac��ַ', 'VARCHAR(40)', '40', 'N', 'N', 'Y', '', '', ''))
        elif field_en == 'TERM_INF':
            change_field_array.append(('MOBILE', u'�ֻ���', 'VARCHAR(16)', '16', 'N', 'N', 'Y', 'IDX_MOBILE', 'reverse(MOBILE,2)', '0,1,2,3,4,5,6,7,8,9'))
            change_field_array.append(('IP', u'IP��ַ', 'VARCHAR(32)', '32', 'N', 'N', 'Y', 'IDX_IP', 'reverse(IP,2)', '0,1,2,3,4,5,6,7,8,9'))
        else:
            if field_en not in ('P9_START_BATCH', 'P9_END_BATCH', 'P9_DEL_FLAG', 'P9_JOB_NAME', 'P9_BATCH_NUMBER', 'P9_DEL_DATE', 'P9_DEL_BATCH', 'P9_SPLIT_BRANCH_CD'):
                change_field_array.append(field)

    return change_field_array


#����Ŀ����Դ���Ӧ����
def define_sor_field(change_field_array):
    sor_field_array = []
    partition_field = ""

    for field in change_field_array:
        (field_en, field_cn, field_type, field_length, field_is_pk, field_is_dk, field_to_ctbase, index_describe, index_function, index_split) = field

        # �����ֶβ������ֶ��б���
        # if field_is_dk == "Y":
        #     partition_field = field_en
        # else:
        sor_field_array.append(field)

    return (sor_field_array, partition_field)


#����Ŀ����Դ���Ӧ����
def define_ctbase_field(change_field_array):
    ctbase_field_array = []
    partition_field = ""

    for field in change_field_array:
        (field_en, field_cn, field_type, field_length, field_is_pk, field_is_dk, field_to_ctbase, index_describe, index_function, index_split) = field

        if field_to_ctbase == 'Y':
            ctbase_field_array.append(field)

            if field_is_dk == "Y":
                partition_field = field_en

    return (ctbase_field_array, partition_field)


class TableDefine():
    """����"""
    def __init__(self, table_en, table_cn, field_array):
        self.table_en = table_en
        self.table_cn = table_cn
        self.field_array = field_array
        self.change_field_array = define_change_field(self.field_array)
        self.sor_field_array, partition_field = define_sor_field(self.change_field_array)
        self.ctbase_field_array, partition_field = define_ctbase_field(self.change_field_array)
        self.partition_field = partition_field


class SlideTableStruct():
    """_H ��̬��Ϣ�� ������"""
    def __init__(self):
 
        self.table_list = [
            #��˽�ͻ���Ϣ
            'T0042_TBPC1010_H', 'T0042_TBPC9030_H', 'T0042_TBPC1510_H', 
            #CCBS ����
            'TODDC_CRCRDCRD_H', 'TODDC_SAACNACN_H',
            #CCBS ��Ա  
            'TODDC_FCMTLR0_H', 
            #������������ϵ��Ա��
            'T0651_CCBINS_INF_H', 'T0651_CCBINS_REL_H', 'T0861_EMPE_H'
        ]

        table_sheet, field_sheet = read_excel()
        self.table_map = read_table_name(table_sheet)
        self.table_field_map = read_field_name(field_sheet)

        self.exist_table_map = {}
        for table_en in self.table_list:
            td = TableDefine(table_en, self.table_map.get(table_en, ''), self.table_field_map[table_en])
            self.exist_table_map[table_en] = td


    def __getattr__(self, table_en):
        # print 'attr=%s' % table_name
        # print dir(self)
        if table_en in self.table_list:
            return self.exist_table_map[table_en]
        else:
            raise "����δ����"


def main():
    # table_sheet, field_sheet = read_excel()
    # table_map = read_table_name(table_sheet)
    # table_field_map = read_field_name(field_sheet)

    table_list = [
        #��˽�ͻ���Ϣ
        'T0042_TBPC1010_H', 'T0042_TBPC9030_H', 'T0042_TBPC1510_H', 
        #ECTIP
        'TODEC_TRAD_FLOW_A', 'TODEC_QUERY_TRAD_FLOW_A', 'TODEC_LOGIN_TRAD_FLOW_A',
        #CCBS ATM
        'TODDC_CRATMATM_SH', 'TODDC_CRATMDET_A',
        #CCBS POS
        'TODDC_CRPOSPOS_H', 'TODDC_CRDETDET_A',
        #CCBS ����
        'TODDC_CRCRDCRD_H', 'TODDC_SAACNACN_H',
        #CCBS ��ϸ��ˮ
        'TODDC_SAACNTXN_A', 'TODDC_SAETXETX_A',
        #CCBS ��Ա  
        'TODDC_FCMTLR0_H', 
        #������������ϵ��Ա��
        'T0651_CCBINS_INF_H', 'T0651_CCBINS_REL_H', 'T0861_EMPE_H',
        #���ÿ�
        'T0281_TBB1PLT0_H', 'T0281_S11T1_BILL_DTL_H', 'T0281_S11T1_BIL_DSP_D0_H'
        ]


    # sts = SlideTableStruct()
    # print sts.TODDC_SAACNACN_H.table_cn
    # print sts.TODDC_SAACNACN_H.table_en
    # print sts.TODDC_SAACNACN_H.sor_field_array
    # print sts.TODDC_SAACNACN_H.ctbase_field_array

    mts = MiddleTableStruct()
    for table_en, td in mts.exist_table_map.items():
        print '-------%s-----' % table_en
        for field in td.ctbase_field_array:
            print field[0], field[1]

    # print mds.MID_CUSTOMER_CUR.ctbase_field_array

    # for table_en in table_list:
    #     table_en = table_en.upper()
    #     table_cn = table_map[table_en]
    #     field_array = table_field_map[table_en]

        # print '%s:%s' % (table_en, table_cn)

        #����CTBase�ű�
        # build_ctbase_create_xml(table_en, table_cn, field_array)
        # build_ctbase_load_script(table_en, table_cn, field_array)
        # #���ɽ�Hive���SQL�ű��ļ�
        # build_hive_create_sql(table_en, table_cn, field_array)

        # if table_en[-2:] == '_A' or table_en in ('T0281_S11T1_BILL_DTL_H', 'T0281_S11T1_BIL_DSP_D0_H'):
        #     build_hive_detail_insert_sql(table_en, table_cn, field_array)
        #     pass
        # else:
        #     # print '%s:%s' % (table_en, table_cn)
        #     build_hive_entity_insert_sql(table_en, table_cn, field_array)

        #     # ����������
        #     build_hive_entity_history_create_sql(table_en, table_cn, field_array)
        #     build_hive_entity_history_insert_sql(table_en, table_cn, field_array)

    # build_makefile(table_list)


if __name__ == '__main__':
    main()
    # a = ['1', '2', '3']
    # b = ['  %s' % x for x in a]

    # print ','.join(b)
