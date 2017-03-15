#!/usr/bin/python
#coding:gbk

import sys, os, re, time
from read_data_struct import *


#���ɽ�Hive���SQL�ű��ļ�
def build_hive_create_sql(td):
    partition = ""
    add_partition = ""

    # ���� �����ⲿ���ֶ�
    ext_field_stmt_list = ['   -- %s\n   %-30s string' % (field[1], field[0]) for field in td.field_array]
    ext_field_stmt = ",\n".join(ext_field_stmt_list)
    sql = u'''use sor;

-- Hive����ű�
-- %(cn)s: %(en)s

-- �ⲿ��
DROP TABLE IF EXISTS EXT_%(en)s;

CREATE EXTERNAL TABLE IF NOT EXISTS EXT_%(en)s(
%(ext_field)s
)
PARTITIONED BY (LOAD_DATE string)
STORED AS TEXTFILE;
''' % {'en':td.table_en, 'cn':td.table_cn, 'ext_field':ext_field_stmt}

    # �����ֶβ������ֶ��б���
    if td.partition_field != "":
        partition = 'PARTITIONED BY (%s string)' % td.partition_field

    # ���� ��Դ���ֶ�
    sor_field_stmt_list = ['   -- %s\n   %-30s string' % (field[1], field[0]) for field in td.sor_field_array]
    sor_field_stmt = ",\n".join(sor_field_stmt_list)

    sql += u'''
-- ORC�ڲ�����Լ�洢�ռ�
DROP TABLE IF EXISTS INN_%(en)s;

CREATE TABLE IF NOT EXISTS INN_%(en)s(
%(cols)s
)
PARTITIONED BY (LOAD_DATE string)
STORED AS ORC;\n
''' % {'en':td.table_en, 'cols':sor_field_stmt}

    # ���� ������ ֻ����CTBase�ֶ�
    ctbase_field_stmt_list = ['   -- %s\n   %-30s string' % (field[1], field[0]) for field in td.ctbase_field_array if field[0] not in ('P9_END_DATE',)]
    ctbase_field_stmt = ",\n".join(ctbase_field_stmt_list)

    sql += u'''
-- �������м�����
DROP TABLE IF EXISTS CT_%(en)s_MID;

CREATE TABLE IF NOT EXISTS CT_%(en)s_MID (
%(cols)s,
   -- P9��������
   P9_END_DATE                    string
)
PARTITIONED BY (%(partition_field)s string)
STORED AS ORC;

-- ����������
DROP TABLE IF EXISTS CT_%(en)s;

CREATE TABLE IF NOT EXISTS CT_%(en)s (
%(cols)s
)
PARTITIONED BY (P9_END_DATE string)
STORED AS ORC;
''' % {'en':td.table_en, 'cols':ctbase_field_stmt, 'partition_field':'DATA_TYPE'}

    # ���ɽ����ļ�
    f = open(r'./hive_create/CREATE_%s.sql' % td.table_en, 'w')
    f.write(sql.encode('utf-8'))
    f.close()


#���ɽ�CTBase���XML�ļ�
def build_ctbase_create_xml(table_en, table_cn, field_array):

    xml = u'''<?xml version="1.0" encoding="UTF-8"?>

<!--������ԴCTBase���������ļ�-->

<table>
    <!-- �۴ر���-->
    <clusterTable>
        <name>CT_%s_CLUS</name>
        <describe>%s</describe>
    </clusterTable>

    <userTable>
        <name>%s</name>
        <describe>%s</describe>
        <columns>''' % (table_en, table_cn, table_en, table_cn)

    # ��Դ���ֶ�ɸѡ
    change_field_array = define_change_field(field_array)
    ctbase_field_array, partition_field = define_ctbase_field(change_field_array)

    # �����ֶ�
    for field in ctbase_field_array:
        (field_en, field_cn, field_type, field_length, field_is_pk, field_is_dk, field_to_ctbase, index_describe, index_function, index_split) = field
        # print '   %s,%s,%s,%s,%s,%s,%s,%s,%s,%s' % (field_en, field_cn, field_type, field_length, field_is_pk, field_is_dk, field_to_ctbase, index_describe, index_function, index_split)
        xml += '''        
            <column col_name="%s">
                <type>DataType.VARCHAR</type>
                <length>%s</length>
                <describe>%s</describe>
            </column>
''' % (field_en, field_length, field_cn)

    xml += u'        </columns>\n        <indexs>\n'

    # ����������
    index_map = {}
    for field in ctbase_field_array:
        (field_en, field_cn, field_type, field_length, field_is_pk, field_is_dk, field_to_ctbase, index_describe, index_function, index_split) = field
        if index_describe == '': continue

        index_describe_array = index_describe.split(':')

        index_name = index_describe_array[0]
        if len(index_describe_array) == 1:
            index_seq = 1
        else:
            index_seq = index_describe_array[1]

        if index_name in index_map.keys():
            index_array = index_map[index_name]
        else:
            index_array = []
            index_map[index_name] = index_array

        index_array.append((index_seq, field_en, index_function, index_split))
    
    # ��������
    pk_index_array = index_map["PK"]
    pk_index_array.sort()
    # ����������������ֶ�
    if "ALL" in index_map.keys():
        all_index_array = index_map["ALL"]

    many_index_list = []
    for index_name, index_array in index_map.items():
        if index_name in ("PK", "ALL"): continue
        # �������ж���������ͬӵ�е���
        if 'all_index_array' in locals():
            index_array.extend(all_index_array)
        index_array.sort()

        many_index_list.append((index_name, index_array))

    # ���ɽ�����ʹ�õ�XML
    many_index_list.sort()
    many_index_list.insert(0, ('PK', pk_index_array))
    for (index_name, index_array) in many_index_list:
        # print index_name, index_array
        xml += '            <index name="%s">\n' % index_name
        for index_desc in index_array:
            (index_seq, field_en, index_function, index_split) = index_desc
            if int(index_seq) == 1:
                if index_function == '': 
                    index_function = 'reverse(%s,2)' % field_en
                if index_split == '':
                    index_split = '0,1,2,3,4,5,6,7,8,9'

            xml += '                <column col_name="%s">\n' % field_en
            xml += '                    <function>%s</function>\n' % index_function
            xml += '                    <splitkey>%s</splitkey>\n' % index_split
            xml += '                </column>\n'
        xml += '            </index>\n'

    xml += '''        </indexs>
    </userTable>
</table>'''

    if not os.path.exists('./ctbase_create'):  #Ŀ¼�����ڣ����½�
        os.mkdir('./ctbase_create')

    f = open(r'./ctbase_create/%s.xml' % table_en, 'w')
    f.write(xml.encode('utf-8'))
    f.close()


#���ɽ�CTBase���XML�ļ�
def build_ctbase_load_script(table_en, table_cn, field_array):
    hiveTable = 'CT_%s' % table_en

    # ��Դ���ֶ�ɸѡ
    change_field_array = define_change_field(field_array)
    ctbase_field_array, partition_field = define_ctbase_field(change_field_array)
    
    i = 0
    partition = ""
    field_str = ""
    for field in ctbase_field_array:
        (field_en, field_cn, field_type, field_length, field_is_pk, field_is_dk, field_to_ctbase, index_describe, index_function, index_split) = field
        if index_describe != '':
            index_describe_array = index_describe.split(':')

            index_name = index_describe_array[0]
            if len(index_describe_array) == 1:
                index_seq = 1
            else:
                index_seq = int(index_describe_array[1])

            # �Ƕ���������һ���ֶΣ�������Ϊ��ʱת�����
            if index_name not in ('PK', 'ALL') and index_seq == 1:
                field_en = 'ts150_Empty2Random(%s)' % field_en

        i += 1
        if i % 5 == 0: field_str += '\n           '
        field_str += field_en + ','

    # ʵ��������������
    entity_condition = "";
    if table_en[-2:] in ("_H", 'SH'):
        entity_condition = '''
       OR (%s='29991231'
           AND P9_START_DATE='${log_date}'
          )''' % partition_field

    # ȥ��field_str���һ��,
    field_str = field_str[:-1]

    script = u'''#!/bin/sh
######################################################
#   ��Hive�ϵ�%(en)s���뵽CTBase��
#                   wuzhaohui@tienon.com
######################################################

#���û���Shell������
source /home/ap/dip/appjob/shelljob/TS150/case_trace/case_trace_base.sh

#���������в���
logdate_arg $*

export_from_hive()
{
    # %(en)s��: %(cn)s
    $hdsrun hdsHive USERNAME:$hadoop_user,INSTANCEID:EXPORT-%(en)s-${log_date}-0001 <<!

    use ts150;

    INSERT OVERWRITE DIRECTORY 'case_trace/%(en)s'
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' WITH SERDEPROPERTIES('serialization.null.format'='')
    SELECT %(field)s
    FROM %(en)s_SHORT
    WHERE %(partition)s='${log_date}' %(entity_condition)s;
!
}

import_to_ctbase()
{
    $hdsrun hbaseBatchLoader USERNAME:$hadoop_user,INSTANCEID:IMPORT-%(en)s-${log_date}-0001 SYS_NAME:TS150,TRDBEGDATE:${log_date},TRDENDDATE:${log_date},DATA_DATE:${log_date},ORG:000000000,INPUT:case_trace/%(en)s/,CLUSTERTABLE:%(en)s_CLUS,USERTABLE:%(table_en)s
}

export_from_hive
import_to_ctbase
''' % {'en':hiveTable, 'cn':table_cn, 'table_en':table_en, 'field':field_str, 
        'partition':partition_field, 'entity_condition':entity_condition}

    if not os.path.exists('./ctbase_load'):  #Ŀ¼�����ڣ����½�
        os.mkdir('./ctbase_load')

    f = open(r'./ctbase_load/%s.sh' % table_en, 'w')
    f.write(script.encode('utf-8'))
    f.close()


def build_makefile(table_list):
    f = open(r'./ctbase_create/makefile.ct', 'w')
    for table_en in table_list:
        f.write('\tjava $(opt_cp) TsUtil TS150 %s\n' % table_en.upper().encode('utf-8'))

    f.write('\n\n')
    for table_en in table_list:
        f.write('hadoop fs -mkdir $hdfsinput/%s\n' % table_en.upper().encode('utf-8'))
        f.write('hadoop fs -chmod -R 755 $hdfsinput/%s\n' % table_en.upper().encode('utf-8'))

    f.write('\n\n')
    for table_en in table_list:
        f.write('beeline -f CREATE_%s.sql\n' % table_en.upper().encode('utf-8'))

    f.write('\n\n')
    entity_list = ''
    detail_list = ''
    for table_en in table_list:
        if table_en[-2:] == '_A' or table_en in ('T0281_S11T1_BILL_DTL_H', 'T0281_S11T1_BIL_DSP_D0_H'):
            detail_list += '%s ' % table_en
        else:
            entity_list += '%s ' % table_en
        
    f.write('entity_list="%s"\n' % entity_list)
    f.write('detail_list="%s"\n' % detail_list)

    f.close()


#���� ����ű� Hive��CTBase��������
def build_hive_entity_history_create_sql(table_en, table_cn, field_array):
    partition = ""
    add_partition = ""

    # ��Դ���ֶ�ɸѡ
    change_field_array = define_change_field(field_array)
    ctbase_field_array, partition_field = define_ctbase_field(change_field_array)

    # ��Hive���ų� �����ֶ�
    # for i in range(len(ctbase_field_array)):
    #     (field_en, field_cn, field_type, field_length, field_is_pk, field_is_dk, field_to_ctbase, index_describe, index_function, index_split) = ctbase_field_array[i]
    #     if field_en == partition_field:
    #         # print i, field_en
    #         del ctbase_field_array[i]
    #         break

    # ���� Hive�� ֻ����CTBase�ֶ�
    table_columns = "\n"
    for field in ctbase_field_array:
        (field_en, field_cn, field_type, field_length, field_is_pk, field_is_dk, field_to_ctbase, index_describe, index_function, index_split) = field

        # ���һ�в���,
        if field == ctbase_field_array[-1]:
            field_line = "   %-30s string\n" % (field_en)
        else:
            field_line = "   %-30s string,\n" % (field_en)

        table_columns += field_line

    # print table_columns
 
    # �����ֶβ������ֶ��б���
    partition_field = 'DATA_TYPE'
    if partition_field != "":
        partition = 'PARTITIONED BY (%s string)' % partition_field
        add_partition = "ALTER TABLE CT_%s_MID ADD PARTITION(%s='SRC');\n" % (table_en, partition_field)
        add_partition += "ALTER TABLE CT_%s_MID ADD PARTITION(%s='CUR_NO_DUP');\n" % (table_en, partition_field)
        add_partition += "ALTER TABLE CT_%s_MID ADD PARTITION(%s='PRE_NO_DUP');\n" % (table_en, partition_field)

    sql = u'''use ts150;

-- ������ԴHive����ű�
-- %(cn)s: %(en)s

DROP TABLE IF EXISTS CT_%(en)s_MID;

CREATE TABLE IF NOT EXISTS CT_%(en)s_MID (%(cols)s)
%(partition)s
STORED AS ORC;\n

%(add_partition)s

DROP TABLE IF EXISTS CT_%(en)s_SHORT;

CREATE TABLE IF NOT EXISTS CT_%(en)s_SHORT (%(cols)s)
STORED AS ORC;\n

''' % {'en':table_en, 'cn':table_cn, 'cols':table_columns,
       'partition':partition, 'add_partition':add_partition}

    if not os.path.exists('./hive_create'):  #Ŀ¼�����ڣ����½�
        os.mkdir('./hive_create')

    f = open(r'./hive_create/CREATE_%s_SHORT.sql' % table_en, 'w')
    f.write(sql.encode('utf-8'))
    f.close()


def main():
    #Ŀ¼�����ڣ����½�
    if not os.path.exists('./hive_create'):  
        os.mkdir('./hive_create')

    sts = SlideTableStruct()
    # build_hive_create_sql(sts.TODDC_CRCRDCRD_H)
    for table_en, td in sts.exist_table_map.items():
        build_hive_create_sql(td)
        print 'table: %s finish' % table_en


if __name__ == '__main__':
    main()
