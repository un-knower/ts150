--输入输出数据
--IN_CUR_HIVE="sor.CT_{{table_en}}_MID"
--OUT_CUR_HIVE="sor.CT_{{table_en}}"
{{
def flat_field(field_list, per_line_field_num=4, per_line_prefix_space_num=10, exclude_field=[], has_suffix=False):
    i = 0
    main_fields = []
    for field_name, field_comment in field_list:
        if field_name in exclude_field:
            continue
        # 每行4个字段
        if i % per_line_field_num == 0:
            sub_fields = []
            main_fields.append(sub_fields)
        sub_fields.append(field_name)
        i += 1

    # 字段间加换行符
    spaces = '                                                                  '
    flat_columns = (',\n%s' % (' '*per_line_prefix_space_num)).join([', '.join(sub_fields) for sub_fields in main_fields])
    if has_suffix:
        return flat_columns + ','

    return flat_columns
}}
use sor;

-- Hive数据拉链处理
-- {{table_cn}}: {{table_en}}

 -- 加入与昨天全量数据，去重
INSERT OVERWRITE TABLE CT_{{table_en}}_MID PARTITION(DATA_TYPE='${log_date}_ALL')
SELECT
{% for field_name, field_comment in filter_field_list %}
    {{'%-35s' % (field_name + (' ' if field_name == filter_field_list[-1][0] else ','))}}  -- {{field_comment}}
{% %}
  FROM (SELECT {{flat_field(filter_field_list, 4, 15, [], True)}}
               row_number() over (partition by
                   {{flat_field(filter_field_list, 4, 19, ['P9_START_DATE', 'P9_END_DATE'])}}
                   order by P9_START_DATE
               ) rownum
         FROM CT_{{table_en}}_MID
        WHERE DATA_TYPE in ('${less_7_date}_ALL', '${log_date}_INC',
              {{','.join(["'${less_%d_date}_INC'" % i for i in range(1,7)])}})
        ) a
 WHERE a.rownum = 1;

-- Hive并行执行参数设置
set hive.exec.parallel=true;

-- 重建拉链
FROM (
SELECT {{flat_field(filter_field_list, 4, 7, ['P9_END_DATE',], True)}}
       lead(P9_START_DATE, 1, '29991231') over (partition by {{pk_fields}}
           order by P9_START_DATE) as P9_END_DATE
  FROM CT_{{table_en}}_MID
 WHERE DATA_TYPE='${log_date}_ALL' ) t

-- 插入指定日期的数据
INSERT OVERWRITE TABLE CT_{{table_en}} PARTITION(P9_END_DATE='${log_date}')
SELECT {{flat_field(filter_field_list, 4, 7, ['P9_END_DATE',])}}
 WHERE P9_END_DATE = '${log_date}'

{% for i in range(1, 7) %}
-- 插入指定日期的数据
INSERT OVERWRITE TABLE CT_{{table_en}} PARTITION(P9_END_DATE='{{"${less_%d_date}" % i}}')
SELECT {{flat_field(filter_field_list, 4, 7, ['P9_END_DATE',])}}
 WHERE P9_END_DATE = '{{"${less_%d_date}" % i}}'

{% %}
-- 插入当前最新数据
INSERT OVERWRITE TABLE CT_{{table_en}} PARTITION(P9_END_DATE='29991231')
SELECT {{flat_field(filter_field_list, 4, 7, ['P9_END_DATE',])}}
 WHERE P9_END_DATE = '29991231';
