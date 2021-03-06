--输入输出数据
--IN_CUR_HIVE="sor.CT_T0042_TBPC1510_H_MID"
--OUT_CUR_HIVE="sor.CT_T0042_TBPC1510_H"
use sor;

-- Hive数据拉链处理
-- 客户联系位置信息: T0042_TBPC1510_H

 -- 加入与昨天全量数据，去重
INSERT OVERWRITE TABLE CT_T0042_TBPC1510_H_MID PARTITION(DATA_TYPE='${log_date}_ALL')
SELECT
    CST_ID,                              -- 客户编号
    CTC_INF_TPCD,                        -- 联系信息类型代码
    TELCTCMOD_DMST_DSTCNO,               -- 电话联系方式国内区号
    TELCTCMOD_NO,                        -- 电话联系方式号码
    TELCTCMOD_EXN_NO,                    -- 电话联系方式分机号码
    ZIPECD,                              -- 邮政编码
    CTYRGON_CD,                          -- 国家地区代码
    PROV_ATNMSRGON_CD,                   -- 省自治区代码
    DTL_ADR_CNTNT,                       -- 详细地址内容
    P9_START_DATE,                       -- P9开始日期
    P9_END_DATE                          -- P9结束日期
  FROM (SELECT CST_ID, CTC_INF_TPCD, TELCTCMOD_DMST_DSTCNO, TELCTCMOD_NO,
               TELCTCMOD_EXN_NO, ZIPECD, CTYRGON_CD, PROV_ATNMSRGON_CD,
               DTL_ADR_CNTNT, P9_START_DATE, P9_END_DATE,
               row_number() over (partition by
                   CST_ID, CTC_INF_TPCD, TELCTCMOD_DMST_DSTCNO, TELCTCMOD_NO,
                   TELCTCMOD_EXN_NO, ZIPECD, CTYRGON_CD, PROV_ATNMSRGON_CD,
                   DTL_ADR_CNTNT
                   order by P9_START_DATE
               ) rownum
         FROM CT_T0042_TBPC1510_H_MID
        WHERE DATA_TYPE in ('${less_7_date}_ALL', '${log_date}_INC',
              '${less_1_date}_INC','${less_2_date}_INC','${less_3_date}_INC','${less_4_date}_INC','${less_5_date}_INC','${less_6_date}_INC')
        ) a
 WHERE a.rownum = 1;

-- Hive并行执行参数设置
set hive.exec.parallel=true;

-- 重建拉链
FROM (
SELECT CST_ID, CTC_INF_TPCD, TELCTCMOD_DMST_DSTCNO, TELCTCMOD_NO,
       TELCTCMOD_EXN_NO, ZIPECD, CTYRGON_CD, PROV_ATNMSRGON_CD,
       DTL_ADR_CNTNT, P9_START_DATE,
       lead(P9_START_DATE, 1, '29991231') over (partition by CST_ID, CTC_INF_TPCD, TELCTCMOD_NO
           order by P9_START_DATE) as P9_END_DATE
  FROM CT_T0042_TBPC1510_H_MID
 WHERE DATA_TYPE='${log_date}_ALL' ) t

-- 插入指定日期的数据
INSERT OVERWRITE TABLE CT_T0042_TBPC1510_H PARTITION(P9_END_DATE='${log_date}')
SELECT CST_ID, CTC_INF_TPCD, TELCTCMOD_DMST_DSTCNO, TELCTCMOD_NO,
       TELCTCMOD_EXN_NO, ZIPECD, CTYRGON_CD, PROV_ATNMSRGON_CD,
       DTL_ADR_CNTNT, P9_START_DATE
 WHERE P9_END_DATE = '${log_date}'

-- 插入指定日期的数据
INSERT OVERWRITE TABLE CT_T0042_TBPC1510_H PARTITION(P9_END_DATE='${less_1_date}')
SELECT CST_ID, CTC_INF_TPCD, TELCTCMOD_DMST_DSTCNO, TELCTCMOD_NO,
       TELCTCMOD_EXN_NO, ZIPECD, CTYRGON_CD, PROV_ATNMSRGON_CD,
       DTL_ADR_CNTNT, P9_START_DATE
 WHERE P9_END_DATE = '${less_1_date}'

-- 插入指定日期的数据
INSERT OVERWRITE TABLE CT_T0042_TBPC1510_H PARTITION(P9_END_DATE='${less_2_date}')
SELECT CST_ID, CTC_INF_TPCD, TELCTCMOD_DMST_DSTCNO, TELCTCMOD_NO,
       TELCTCMOD_EXN_NO, ZIPECD, CTYRGON_CD, PROV_ATNMSRGON_CD,
       DTL_ADR_CNTNT, P9_START_DATE
 WHERE P9_END_DATE = '${less_2_date}'

-- 插入指定日期的数据
INSERT OVERWRITE TABLE CT_T0042_TBPC1510_H PARTITION(P9_END_DATE='${less_3_date}')
SELECT CST_ID, CTC_INF_TPCD, TELCTCMOD_DMST_DSTCNO, TELCTCMOD_NO,
       TELCTCMOD_EXN_NO, ZIPECD, CTYRGON_CD, PROV_ATNMSRGON_CD,
       DTL_ADR_CNTNT, P9_START_DATE
 WHERE P9_END_DATE = '${less_3_date}'

-- 插入指定日期的数据
INSERT OVERWRITE TABLE CT_T0042_TBPC1510_H PARTITION(P9_END_DATE='${less_4_date}')
SELECT CST_ID, CTC_INF_TPCD, TELCTCMOD_DMST_DSTCNO, TELCTCMOD_NO,
       TELCTCMOD_EXN_NO, ZIPECD, CTYRGON_CD, PROV_ATNMSRGON_CD,
       DTL_ADR_CNTNT, P9_START_DATE
 WHERE P9_END_DATE = '${less_4_date}'

-- 插入指定日期的数据
INSERT OVERWRITE TABLE CT_T0042_TBPC1510_H PARTITION(P9_END_DATE='${less_5_date}')
SELECT CST_ID, CTC_INF_TPCD, TELCTCMOD_DMST_DSTCNO, TELCTCMOD_NO,
       TELCTCMOD_EXN_NO, ZIPECD, CTYRGON_CD, PROV_ATNMSRGON_CD,
       DTL_ADR_CNTNT, P9_START_DATE
 WHERE P9_END_DATE = '${less_5_date}'

-- 插入指定日期的数据
INSERT OVERWRITE TABLE CT_T0042_TBPC1510_H PARTITION(P9_END_DATE='${less_6_date}')
SELECT CST_ID, CTC_INF_TPCD, TELCTCMOD_DMST_DSTCNO, TELCTCMOD_NO,
       TELCTCMOD_EXN_NO, ZIPECD, CTYRGON_CD, PROV_ATNMSRGON_CD,
       DTL_ADR_CNTNT, P9_START_DATE
 WHERE P9_END_DATE = '${less_6_date}'

-- 插入当前最新数据
INSERT OVERWRITE TABLE CT_T0042_TBPC1510_H PARTITION(P9_END_DATE='29991231')
SELECT CST_ID, CTC_INF_TPCD, TELCTCMOD_DMST_DSTCNO, TELCTCMOD_NO,
       TELCTCMOD_EXN_NO, ZIPECD, CTYRGON_CD, PROV_ATNMSRGON_CD,
       DTL_ADR_CNTNT, P9_START_DATE
 WHERE P9_END_DATE = '29991231';
