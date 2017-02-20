use sor;
-- 客户联系位置信息 T0042_TBPC1510_H 拉链处理

-- 复制贴源数据
INSERT OVERWRITE TABLE INN_T0042_TBPC1510_H_MID PARTITION(DATA_TYPE='SRC')
SELECT 
       -- 客户编号
       a.CST_ID,
       -- 联系信息类型代码
       a.CTC_INF_TPCD,
       -- 电话联系方式号码
       a.TELCTCMOD_NO,
       -- 电话联系方式分机号码
       a.TELCTCMOD_EXN_NO,
       -- 详细地址内容
       a.DTL_ADR_CNTNT,
       -- P9开始日期
       a.P9_START_DATE,
       -- P9结束日期
       a.P9_END_DATE
  FROM EXT_T0042_TBPC1510_H a
 WHERE LOAD_DATE='${log_date}';

-- 去重
INSERT OVERWRITE TABLE CT_T0042_TBPC1510_H_MID PARTITION(DATA_TYPE='CUR_NO_DUP')
SELECT 
       -- 客户编号
       a.CST_ID,
       -- 联系信息类型代码
       a.CTC_INF_TPCD,
       -- 电话联系方式号码
       a.TELCTCMOD_NO,
       -- 电话联系方式分机号码
       a.TELCTCMOD_EXN_NO,
       -- 详细地址内容
       a.DTL_ADR_CNTNT,
       -- P9开始日期
       a.P9_START_DATE,
       -- P9结束日期
       a.P9_END_DATE
  FROM (SELECT , P9_START_DATE, P9_END_DATE,
               row_number() over (
                    partition by 
                    order by P9_START_DATE
                   ) rownum
         FROM CT_T0042_TBPC1510_H_MID 
        WHERE DATA_TYPE in ('SRC', 'PRE_NO_DUP') 
        ) a
 WHERE a.rownum = 1;

-- Hive动态分区参数设置
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=strick;

-- 重建拉链
INSERT OVERWRITE TABLE CT_T0042_TBPC1510_H PARTITION(P9_END_DATE)
SELECT , P9_START_DATE, 
       lead(P9_START_DATE, 1, '29991231') over (partition by CST_ID, CTC_INF_TPCD order by P9_START_DATE) as P9_END_DATE
  FROM CT_T0042_TBPC1510_H_MID
 WHERE DATA_TYPE='CUR_NO_DUP';

------------ 以下操作可以导致数据丢失 ---------------
-- 备份当前非重复数据 到 PRE_NO_DUP 分区
ALTER TABLE CT_T0042_TBPC1510_H_MID DROP IF EXISTS PARTITION(DATA_TYPE='PRE_NO_DUP');

ALTER TABLE CT_T0042_TBPC1510_H_MID PARTITION(DATA_TYPE='CUR_NO_DUP') 
   RENAME TO PARTITION(DATA_TYPE='PRE_NO_DUP');

ALTER TABLE CT_T0042_TBPC1510_H_MID ADD IF NOT EXISTS PARTITION(DATA_TYPE='CUR_NO_DUP');
