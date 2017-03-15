use sor;
-- 内外客户号对照信息 T0042_TBPC9030_H 拉链处理

-- 复制贴源数据
INSERT OVERWRITE TABLE CT_T0042_TBPC9030_H_MID PARTITION(DATA_TYPE='${log_date}_INC')
SELECT 
       -- #源信息系统代码
       a.SRC_INF_STM_CD,
       -- #源系统客户编号
       a.SRCSYS_CST_ID,
       -- 客户编号
       a.CST_ID,
       -- P9开始日期
       a.P9_START_DATE,
       -- P9结束日期
       a.P9_END_DATE
  FROM INN_T0042_TBPC9030_H a
 WHERE LOAD_DATE='${log_date}';

-- 去重
INSERT OVERWRITE TABLE CT_T0042_TBPC9030_H_MID PARTITION(DATA_TYPE='${log_date}_ALL')
SELECT 
       -- #源信息系统代码
       a.SRC_INF_STM_CD,
       -- #源系统客户编号
       a.SRCSYS_CST_ID,
       -- 客户编号
       a.CST_ID,
       -- P9开始日期
       a.P9_START_DATE,
       -- P9结束日期
       a.P9_END_DATE
  FROM (SELECT SRC_INF_STM_CD, SRCSYS_CST_ID, CST_ID, 
               P9_START_DATE, P9_END_DATE,
               row_number() over (
                    partition by SRC_INF_STM_CD, SRCSYS_CST_ID, CST_ID
                    order by P9_START_DATE
                   ) rownum
         FROM CT_T0042_TBPC9030_H_MID 
        WHERE DATA_TYPE in ('${log_date}_INC', '${log_date_less_1}_ALL') 
        ) a
 WHERE a.rownum = 1;

-- Hive动态分区参数设置
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=strick;

-- 重建拉链
INSERT OVERWRITE TABLE CT_T0042_TBPC9030_H PARTITION(P9_END_DATE)
SELECT SRC_INF_STM_CD, SRCSYS_CST_ID, CST_ID, 
       P9_START_DATE, 
       lead(P9_START_DATE, 1, '29991231') over (partition by SRC_INF_STM_CD, SRCSYS_CST_ID order by P9_START_DATE) as P9_END_DATE
  FROM CT_T0042_TBPC9030_H_MID
 WHERE DATA_TYPE='${log_date}_ALL';
