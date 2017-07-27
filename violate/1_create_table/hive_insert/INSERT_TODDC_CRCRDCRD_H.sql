use sor;
-- 银行卡主档 TODDC_CRCRDCRD_H 拉链处理

-- 复制贴源数据
INSERT OVERWRITE TABLE CT_TODDC_CRCRDCRD_H_MID PARTITION(DATA_TYPE='${log_date}_INC')
SELECT 
       -- 卡号
       a.CR_CRD_NO,
       -- 挂失日期
       a.CR_DL_DT,
       -- 挂失状态
       a.CR_DL_STS,
       -- 借贷别
       a.CR_DR_CR_COD,
       -- 卡种代码
       a.CR_CRD_TYP_COD,
       -- 卡状态
       a.CR_CRD_STS,
       -- 客户编号
       a.CR_CUST_NO,
       -- 营业单位代码
       a.CR_OPUN_COD,
       -- 主附卡标志
       a.CR_MNSUB_DRP,
       -- 主卡卡号
       a.CR_PCRD_NO,
       -- 注销日期
       a.CR_CNCLC_DT,
       -- 注销状态
       a.CR_CNCL_STS,
       -- 更改日期
       a.CR_CHG_GRNTR_DT,
       -- 开卡日期
       a.CR_OPCR_DATE,
       -- 发卡机构号
       a.CR_APP_NO,
       -- 换卡日期
       a.CR_CRD_CHG_DT,
       -- 换卡机构号
       a.CR_CHG_OPUN_COD,
       -- P9开始日期
       a.P9_START_DATE,
       -- P9结束日期
       a.P9_END_DATE
  FROM INN_TODDC_CRCRDCRD_H a
 WHERE LOAD_DATE='${log_date}';

-- 去重
INSERT OVERWRITE TABLE CT_TODDC_CRCRDCRD_H_MID PARTITION(DATA_TYPE='${log_date}_ALL')
SELECT 
       -- 卡号
       a.CR_CRD_NO,
       -- 挂失日期
       a.CR_DL_DT,
       -- 挂失状态
       a.CR_DL_STS,
       -- 借贷别
       a.CR_DR_CR_COD,
       -- 卡种代码
       a.CR_CRD_TYP_COD,
       -- 卡状态
       a.CR_CRD_STS,
       -- 客户编号
       a.CR_CUST_NO,
       -- 营业单位代码
       a.CR_OPUN_COD,
       -- 主附卡标志
       a.CR_MNSUB_DRP,
       -- 主卡卡号
       a.CR_PCRD_NO,
       -- 注销日期
       a.CR_CNCLC_DT,
       -- 注销状态
       a.CR_CNCL_STS,
       -- 更改日期
       a.CR_CHG_GRNTR_DT,
       -- 开卡日期
       a.CR_OPCR_DATE,
       -- 发卡机构号
       a.CR_APP_NO,
       -- 换卡日期
       a.CR_CRD_CHG_DT,
       -- 换卡机构号
       a.CR_CHG_OPUN_COD,
       -- P9开始日期
       a.P9_START_DATE,
       -- P9结束日期
       a.P9_END_DATE
  FROM (SELECT CR_CRD_NO, CR_DL_DT, CR_DL_STS, CR_DR_CR_COD,
               CR_CRD_TYP_COD, CR_CRD_STS, CR_CUST_NO, CR_OPUN_COD,
               CR_MNSUB_DRP, CR_PCRD_NO, CR_CNCLC_DT, CR_CNCL_STS,
               CR_CHG_GRNTR_DT, CR_OPCR_DATE, CR_APP_NO, CR_CRD_CHG_DT,
               CR_CHG_OPUN_COD, 
               P9_START_DATE, P9_END_DATE,
               row_number() over (
                    partition by CR_CRD_NO, CR_DL_DT, CR_DL_STS, CR_DR_CR_COD,
               CR_CRD_TYP_COD, CR_CRD_STS, CR_CUST_NO, CR_OPUN_COD,
               CR_MNSUB_DRP, CR_PCRD_NO, CR_CNCLC_DT, CR_CNCL_STS,
               CR_CHG_GRNTR_DT, CR_OPCR_DATE, CR_APP_NO, CR_CRD_CHG_DT,
               CR_CHG_OPUN_COD
                    order by P9_START_DATE
                   ) rownum
         FROM CT_TODDC_CRCRDCRD_H_MID 
        WHERE DATA_TYPE in ('${log_date}_INC', '${log_date_less_1}_ALL') 
        ) a
 WHERE a.rownum = 1;

-- Hive动态分区参数设置
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=strick;

-- 重建拉链
INSERT OVERWRITE TABLE CT_TODDC_CRCRDCRD_H PARTITION(P9_END_DATE)
SELECT CR_CRD_NO, CR_DL_DT, CR_DL_STS, CR_DR_CR_COD,
               CR_CRD_TYP_COD, CR_CRD_STS, CR_CUST_NO, CR_OPUN_COD,
               CR_MNSUB_DRP, CR_PCRD_NO, CR_CNCLC_DT, CR_CNCL_STS,
               CR_CHG_GRNTR_DT, CR_OPCR_DATE, CR_APP_NO, CR_CRD_CHG_DT,
               CR_CHG_OPUN_COD, 
       P9_START_DATE, 
       lead(P9_START_DATE, 1, '29991231') over (partition by CR_CRD_NO order by P9_START_DATE) as P9_END_DATE
  FROM CT_TODDC_CRCRDCRD_H_MID
 WHERE DATA_TYPE='${log_date}_ALL';
