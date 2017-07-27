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
{% for field_name, field_comment in ctbase_field_list %}
    {{'%-25s' % field_name}} string comment '{{field_comment}}'{{',' if field_name != ctbase_field_list[-1][0] else ''}}
{% %}
)
PARTITIONED BY ({{partition_field}} string)
STORED AS ORC;
