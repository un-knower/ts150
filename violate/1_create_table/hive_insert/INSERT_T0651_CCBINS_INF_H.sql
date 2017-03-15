use sor;
-- 建行机构信息 T0651_CCBINS_INF_H 拉链处理

-- 复制贴源数据
INSERT OVERWRITE TABLE CT_T0651_CCBINS_INF_H_MID PARTITION(DATA_TYPE='${log_date}_INC')
SELECT 
       -- 建行机构编号
       a.CCBINS_ID,
       -- 建行机构中文全称
       a.CCBINS_CHN_FULLNM,
       -- 建行机构中文简称
       a.CCBINS_CHN_SHRTNM,
       -- 建行机构英文全称
       a.CCBINS_ENG_FULLNM,
       -- 建行机构英文简称
       a.CCBINS_ENG_SHRTNM,
       -- 机构状态代码
       a.INST_STCD,
       -- P9开始日期
       a.P9_START_DATE,
       -- P9结束日期
       a.P9_END_DATE
  FROM INN_T0651_CCBINS_INF_H a
 WHERE LOAD_DATE='${log_date}';

-- 去重
INSERT OVERWRITE TABLE CT_T0651_CCBINS_INF_H_MID PARTITION(DATA_TYPE='${log_date}_ALL')
SELECT 
       -- 建行机构编号
       a.CCBINS_ID,
       -- 建行机构中文全称
       a.CCBINS_CHN_FULLNM,
       -- 建行机构中文简称
       a.CCBINS_CHN_SHRTNM,
       -- 建行机构英文全称
       a.CCBINS_ENG_FULLNM,
       -- 建行机构英文简称
       a.CCBINS_ENG_SHRTNM,
       -- 机构状态代码
       a.INST_STCD,
       -- P9开始日期
       a.P9_START_DATE,
       -- P9结束日期
       a.P9_END_DATE
  FROM (SELECT CCBINS_ID, CCBINS_CHN_FULLNM, CCBINS_CHN_SHRTNM, CCBINS_ENG_FULLNM,
               CCBINS_ENG_SHRTNM, INST_STCD, 
               P9_START_DATE, P9_END_DATE,
               row_number() over (
                    partition by CCBINS_ID, CCBINS_CHN_FULLNM, CCBINS_CHN_SHRTNM, CCBINS_ENG_FULLNM,
               CCBINS_ENG_SHRTNM, INST_STCD
                    order by P9_START_DATE
                   ) rownum
         FROM CT_T0651_CCBINS_INF_H_MID 
        WHERE DATA_TYPE in ('${log_date}_INC', '${log_date_less_1}_ALL') 
        ) a
 WHERE a.rownum = 1;

-- Hive动态分区参数设置
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=strick;

-- 重建拉链
INSERT OVERWRITE TABLE CT_T0651_CCBINS_INF_H PARTITION(P9_END_DATE)
SELECT CCBINS_ID, CCBINS_CHN_FULLNM, CCBINS_CHN_SHRTNM, CCBINS_ENG_FULLNM,
               CCBINS_ENG_SHRTNM, INST_STCD, 
       P9_START_DATE, 
       lead(P9_START_DATE, 1, '29991231') over (partition by CCBINS_ID order by P9_START_DATE) as P9_END_DATE
  FROM CT_T0651_CCBINS_INF_H_MID
 WHERE DATA_TYPE='${log_date}_ALL';
