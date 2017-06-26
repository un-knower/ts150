--输入输出数据
--IN_CUR_HIVE="sor.{{master_table_en}}"
--IN_CUR_HIVE="sor.{{slave_table_en}}"
--OUT_CUR_HIVE="sor.{{table_en}}"

use sor;

-- 中间表插入
-- {{table_cn}}: {{table_en}}
--    数据来源于：{{master_table_cn}}: {{master_table_en}}
--                {{slave_table_cn}}: {{slave_table_en}}

-- 有关联的数据插入
INSERT OVERWRITE TABLE {{table_en}} PARTITION({{partition_field}}='${log_date}')
SELECT
{% for field_name, field_comment in ctbase_field_list %}
    {{'%-35s' % (field_name + (' ' if field_name == ctbase_field_list[-1][0] else ','))}}  -- {{field_comment}}
{% %}
  FROM
      (SELECT
{% for field_name, field_comment in master_ctbase_field_list %}
           {{'%-35s' % (field_name + (' ' if field_name == master_ctbase_field_list[-1][0] else ','))}}  -- {{field_comment}}
{% %}
         FROM {{master_table_en}}
        WHERE {{master_where}}
          AND {{master_pk_field}} is not null
          AND {{master_pk_field}} <> '')
        ) t1
 INNER JOIN
       (SELECT
{% for field_name, field_comment in slave_ctbase_field_list %}
           {{'%-35s' % (field_name + (' ' if field_name == slave_ctbase_field_list[-1][0] else ','))}}  -- {{field_comment}}
{% %}
         FROM {{slave_table_en}}
        WHERE {{slave_where}}
          AND ({{slave_pk_field}} is not null
          AND {{slave_pk_field}} <> ''
        ) t2
   ON t1.{{master_pk_field}} = t2.{{slave_pk_field}};


-- 追加主表为空的记录
INSERT INTO TABLE {{table_en}} PARTITION({{partition_field}}='${log_date}')
SELECT
{% for field_name, field_comment in master_ctbase_field_list %}
    {{'%-35s' % (field_name + ',')}}  -- {{field_comment}}
{% %}
{% for field_name, field_comment in slave_ctbase_field_list %}
    {{'%-35s' % ( 'null as ' + field_name + (' ' if field_name == slave_ctbase_field_list[-1][0] else ','))}}  -- {{field_comment}}
{% %}
  FROM {{master_table_en}}
 WHERE {{master_where}}
   AND ({{master_pk_field}} is null OR {{master_pk_field}} = '');
