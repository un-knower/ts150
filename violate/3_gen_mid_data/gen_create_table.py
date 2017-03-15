#!/usr/bin/python
#coding:gbk

import sys, os, re, time

sys.path.append("../1_create_table/")
from read_data_struct import *


class MiddleTableStruct():
    """_H 静态信息中间表"""
    def __init__(self):
 
        self.table_join_map = {
            #对私客户信息
            # 'MID_CUSTOMER_CUR':(u'对私客户信息整合表，带原系统客户号', ('T0042_TBPC9030_H', 'T0042_TBPC1010_H')),
            #卡与帐户中间表
            # 'MID_CARD_CUR':(u'CCBS帐户与卡整合表', ('TODDC_SAACNACN_H','TODDC_CRCRDCRD_H')), 
            #客户与卡中间表
            # 'MID_CARD_CUSTOMER_CUR':(u'CCBS账户与客户信息整合表', ('MID_CARD_CUR', 'MID_CUSTOMER_CUR')),
            #机构表、机构关系、员工
            'MID_CCBINS_CUR':(u'机构表', ('T0651_CCBINS_INF_H', 'T0651_CCBINS_REL_H')), 

            # 'MID_EMPE_CCBINS_CUR':(u'员工机构信息表', ('T0861_EMPE_H', 'MID_CCBINS_CUR')),
            # 'MID_FCMTLR0_CCBINS_CUR':(u'柜员机构信息表', ('TODDC_FCMTLR0_H', 'MID_CCBINS_CUR')),
        }
        self.sts = SlideTableStruct()

        # 从贴源表生成的中间表
        self.exist_table_map = {}
        for target_table, (table_cn, src_join_set) in self.table_join_map.items():
            all_field_list = []
            for src_table in src_join_set:
                # 源表为贴源数据
                if src_table in self.sts.exist_table_map.keys():
                    td = self.sts.exist_table_map[src_table]
                    self.exist_table_map[src_table] = td
                    all_field_list.extend(td.field_array)

            filter_field_list = [x for x in all_field_list if x[0] not in ('P9_START_DATE', 'P9_END_DATE')]
            target_td = TableDefine(target_table, table_cn, filter_field_list)
            self.exist_table_map[target_table] = target_td

        # 从中间表生成的下一步中间表
        for target_table, (table_cn, src_join_set) in self.table_join_map.items():
            all_field_list = []
            for src_table in src_join_set:
                # 源表为中间表数据
                if src_table in self.exist_table_map.keys():
                    td = self.exist_table_map[src_table]
                    all_field_list.extend(td.field_array)

                # 源表为贴源数据
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
            raise "表名未定义"


#生成建Hive表的SQL脚本文件
def build_hive_create_sql(td):
    # 生成 中间表字段
    ctbase_field_stmt_list = ['   -- %s\n   %-30s string' % (field[1], field[0]) for field in td.ctbase_field_array]
    ctbase_field_stmt = ",\n".join(ctbase_field_stmt_list)

    sql = u'''use sor;

-- Hive建表脚本
-- %(cn)s: %(en)s

DROP TABLE IF EXISTS %(en)s;

CREATE TABLE IF NOT EXISTS %(en)s(
%(cols)s
)
STORED AS ORC;
''' % {'en':td.table_en, 'cn':td.table_cn, 'cols':ctbase_field_stmt}

    # 生成建表文件
    f = open(r'./hive_create/CREATE_%s.sql' % td.table_en, 'w')
    f.write(sql.encode('utf-8'))
    f.close()


def main():
    #目录不存在，则新建
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
