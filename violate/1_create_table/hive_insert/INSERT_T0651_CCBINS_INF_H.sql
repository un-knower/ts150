use sor;
-- 建行机构信息 T0651_CCBINS_INF_H 拉链处理

-- 复制贴源数据
INSERT OVERWRITE TABLE INN_T0651_CCBINS_INF_H_MID PARTITION(DATA_TYPE='SRC')
SELECT 
       -- 建行机构编号
       a.CCBINS_ID,
       -- 建行机构中文全称
       a.CCBINS_CHN_FULLNM,
       -- 建行机构中文简称
       a.CCBINS_CHN_SHRTNM,
       -- 机构状态代码
       a.INST_STCD
  FROM EXT_T0651_CCBINS_INF_H a
 WHERE LOAD_DATE='${log_date}';

-- 去重
INSERT OVERWRITE TABLE CT_T0651_CCBINS_INF_H_MID PARTITION(DATA_TYPE='CUR_NO_DUP')
SELECT 
       -- 建行机构编号
       a.CCBINS_ID,
       -- 建行机构中文全称
       a.CCBINS_CHN_FULLNM,
       -- 建行机构中文简称
       a.CCBINS_CHN_SHRTNM,
       -- 机构状态代码
       a.INST_STCD
  FROM (SELECT , P9_START_DATE, P9_END_DATE,
               row_number() over (
                    partition by 
                    order by P9_START_DATE
                   ) rownum
         FROM CT_T0651_CCBINS_INF_H_MID 
        WHERE DATA_TYPE in ('SRC', 'PRE_NO_DUP') 
        ) a
 WHERE a.rownum = 1;

-- Hive动态分区参数设置
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=strick;

-- 重建拉链
INSERT OVERWRITE TABLE CT_T0651_CCBINS_INF_H PARTITION(P9_END_DATE)
SELECT , P9_START_DATE, 
       lead(P9_START_DATE, 1, '29991231') over (partition by CCBINS_ID order by P9_START_DATE) as P9_END_DATE
  FROM CT_T0651_CCBINS_INF_H_MID
 WHERE DATA_TYPE='CUR_NO_DUP';

------------ 以下操作可以导致数据丢失 ---------------
-- 备份当前非重复数据 到 PRE_NO_DUP 分区
ALTER TABLE CT_T0651_CCBINS_INF_H_MID DROP IF EXISTS PARTITION(DATA_TYPE='PRE_NO_DUP');

ALTER TABLE CT_T0651_CCBINS_INF_H_MID PARTITION(DATA_TYPE='CUR_NO_DUP') 
   RENAME TO PARTITION(DATA_TYPE='PRE_NO_DUP');

ALTER TABLE CT_T0651_CCBINS_INF_H_MID ADD IF NOT EXISTS PARTITION(DATA_TYPE='CUR_NO_DUP');
