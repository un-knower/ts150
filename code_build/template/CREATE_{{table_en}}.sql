use sor;

-- Hive建表脚本--中间表
-- {{table_cn}}: {{table_en}}

-- 中间表
DROP TABLE IF EXISTS {{table_en}};

CREATE TABLE IF NOT EXISTS {{table_en}} (
{% for field_name, field_comment in ctbase_field_list %}
    {{'%-25s' % field_name}} string comment '{{field_comment}}'{{',' if field_name != ctbase_field_list[-1][0] else ''}}
{% %}
)
PARTITIONED BY ({{partition_field}} string)
STORED AS ORC;
