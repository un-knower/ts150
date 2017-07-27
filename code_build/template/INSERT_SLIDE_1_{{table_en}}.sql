--输入输出数据
--IN_CUR_HDFS="/bigdata/input/TS150/case_trace/{{table_en}}"
--OUT_CUR_HIVE="sor.CT_{{table_en}}_MID"
use sor;

-- Hive贴源数据处理
-- {{table_cn}}: {{table_en}}

-- 指定新数据日期分区位置
ALTER TABLE EXT_{{table_en}} DROP IF EXISTS PARTITION(LOAD_DATE='${log_date}');
ALTER TABLE EXT_{{table_en}} ADD PARTITION(LOAD_DATE='${log_date}')
            LOCATION 'hdfs://hacluster/bigdata/input/TS150/case_trace/{{table_en}}/${log_date}/';

-- 备份贴源数据到ORC内部表
INSERT OVERWRITE TABLE INN_{{table_en}} PARTITION(LOAD_DATE='${log_date}')
SELECT
{% for field_name, field_comment in sor_field_list %}
    {{'%-40s' % (field_name + (' ' if field_name == sor_field_list[-1][0] else ','))}}-- {{field_comment}}
{% %}
  FROM EXT_{{table_en}}
 WHERE LOAD_DATE='${log_date}';

-- 复制当天增量数据
INSERT OVERWRITE TABLE CT_{{table_en}}_MID PARTITION(DATA_TYPE='${log_date}_INC')
SELECT
{% for field_name, field_comment in filter_field_list %}
    {{'%-35s' % (field_name + (' ' if field_name == filter_field_list[-1][0] else ','))}}  -- {{field_comment}}
{% %}
  FROM INN_{{table_en}}
 WHERE LOAD_DATE='${log_date}';
