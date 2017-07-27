use sor;

-- Hive建表脚本
-- {{table_cn}}: {{table_en}}

-- 外部表
DROP TABLE IF EXISTS EXT_{{table_en}};

CREATE EXTERNAL TABLE IF NOT EXISTS EXT_{{table_en}}(
{% for field_name, field_comment in ext_field_list %}
    {{'%-25s' % field_name}} string comment '{{field_comment}}'{{',' if field_name != ext_field_list[-1][0] else ''}}
{% %}
)
PARTITIONED BY (LOAD_DATE string)
STORED AS TEXTFILE;

-- ORC内部表，节约存储空间
DROP TABLE IF EXISTS INN_{{table_en}};

CREATE TABLE IF NOT EXISTS INN_{{table_en}}(
{% for field_name, field_comment in sor_field_list %}
    {{'%-25s' % field_name}} string comment '{{field_comment}}'{{',' if field_name != sor_field_list[-1][0] else ''}}
{% %}
)
PARTITIONED BY (LOAD_DATE string)
STORED AS ORC;

-- 拉链表中间数据
DROP TABLE IF EXISTS CT_{{table_en}}_MID;

CREATE TABLE IF NOT EXISTS CT_{{table_en}}_MID (
{% for field_name, field_comment in filter_field_list %}
    {{'%-25s' % field_name}} string comment '{{field_comment}}'{{',' if field_name != filter_field_list[-1][0] else ''}}
{% %}
)
PARTITIONED BY (DATA_TYPE string)
STORED AS ORC;

-- 最终拉链表
DROP TABLE IF EXISTS CT_{{table_en}};

CREATE TABLE IF NOT EXISTS CT_{{table_en}} (
{{
tmp_field_list = [x for x in filter_field_list if x[0] != partition_field]
}}
{% for field_name, field_comment in tmp_field_list %}
    {{'%-25s' % field_name}} string comment '{{field_comment}}'{{',' if field_name != tmp_field_list[-1][0] else ''}}
{% %}
)
PARTITIONED BY ({{partition_field}} string)
STORED AS ORC;
