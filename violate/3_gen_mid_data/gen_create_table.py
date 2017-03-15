#!/usr/bin/python
#coding:gbk

import sys, os, re, time

sys.path.append("../1_create_table/")
from read_data_struct import *


class MiddleTableStruct():
    """_H ��̬��Ϣ�м��"""
    def __init__(self):
 
        self.table_join_map = {
            #��˽�ͻ���Ϣ
            # 'MID_CUSTOMER_CUR':(u'��˽�ͻ���Ϣ���ϱ���ԭϵͳ�ͻ���', ('T0042_TBPC9030_H', 'T0042_TBPC1010_H')),
            #�����ʻ��м��
            # 'MID_CARD_CUR':(u'CCBS�ʻ��뿨���ϱ�', ('TODDC_SAACNACN_H','TODDC_CRCRDCRD_H')), 
            #�ͻ��뿨�м��
            # 'MID_CARD_CUSTOMER_CUR':(u'CCBS�˻���ͻ���Ϣ���ϱ�', ('MID_CARD_CUR', 'MID_CUSTOMER_CUR')),
            #������������ϵ��Ա��
            'MID_CCBINS_CUR':(u'������', ('T0651_CCBINS_INF_H', 'T0651_CCBINS_REL_H')), 

            # 'MID_EMPE_CCBINS_CUR':(u'Ա��������Ϣ��', ('T0861_EMPE_H', 'MID_CCBINS_CUR')),
            # 'MID_FCMTLR0_CCBINS_CUR':(u'��Ա������Ϣ��', ('TODDC_FCMTLR0_H', 'MID_CCBINS_CUR')),
        }
        self.sts = SlideTableStruct()

        # ����Դ�����ɵ��м��
        self.exist_table_map = {}
        for target_table, (table_cn, src_join_set) in self.table_join_map.items():
            all_field_list = []
            for src_table in src_join_set:
                # Դ��Ϊ��Դ����
                if src_table in self.sts.exist_table_map.keys():
                    td = self.sts.exist_table_map[src_table]
                    self.exist_table_map[src_table] = td
                    all_field_list.extend(td.field_array)

            filter_field_list = [x for x in all_field_list if x[0] not in ('P9_START_DATE', 'P9_END_DATE')]
            target_td = TableDefine(target_table, table_cn, filter_field_list)
            self.exist_table_map[target_table] = target_td

        # ���м�����ɵ���һ���м��
        for target_table, (table_cn, src_join_set) in self.table_join_map.items():
            all_field_list = []
            for src_table in src_join_set:
                # Դ��Ϊ�м������
                if src_table in self.exist_table_map.keys():
                    td = self.exist_table_map[src_table]
                    all_field_list.extend(td.field_array)

                # Դ��Ϊ��Դ����
                # if src_table in self.sts.exist_table_map.keys():
                #     td = self.sts.exist_table_map[src_table]
                #     all_field_list.extend(td.field_array)

            filter_field_list = [x for x in all_field_list if x[0] not in ('P9_START_DATE', 'P9_END_DATE')]
            target_td = TableDefine(target_table, table_cn, filter_field_list)
            self.exist_table_map[target_table] = target_td


    def __getattr__(self, table_en):
        if table_en in self.exist_table_map:
            return self.exist_table_map[table_en]
        else:
            raise "����δ����"


#���ɽ�Hive���SQL�ű��ļ�
def build_hive_create_sql(td):
    # ���� �м���ֶ�
    ctbase_field_stmt_list = ['   -- %s\n   %-30s string' % (field[1], field[0]) for field in td.ctbase_field_array]
    ctbase_field_stmt = ",\n".join(ctbase_field_stmt_list)

    sql = u'''use sor;

-- Hive����ű�
-- %(cn)s: %(en)s

DROP TABLE IF EXISTS %(en)s;

CREATE TABLE IF NOT EXISTS %(en)s(
%(cols)s
)
STORED AS ORC;
''' % {'en':td.table_en, 'cn':td.table_cn, 'cols':ctbase_field_stmt}

    # ���ɽ����ļ�
    f = open(r'./hive_create/CREATE_%s.sql' % td.table_en, 'w')
    f.write(sql.encode('utf-8'))
    f.close()


def main():
    #Ŀ¼�����ڣ����½�
    if not os.path.exists('./hive_create'):  
        os.mkdir('./hive_create')

    mts = MiddleTableStruct()
    # build_hive_create_sql(mts.TODDC_CRCRDCRD_H)
    for table_en, td in mts.exist_table_map.items():
        if table_en[0:4] == 'MID_':
            build_hive_create_sql(td)
            print 'table: %s finish' % table_en


if __name__ == '__main__':
    main()
