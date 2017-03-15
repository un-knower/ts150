use sor;
-- 个人活期存款主档 TODDC_SAACNACN_H 拉链处理

-- 复制贴源数据
INSERT OVERWRITE TABLE CT_TODDC_SAACNACN_H_MID PARTITION(DATA_TYPE='${log_date}_INC')
SELECT 
       -- 帐号
       a.SA_ACCT_NO,
       -- 开户金额
       a.SA_OPAC_AMT,
       -- 开户日期
       a.SA_OPAC_DT,
       -- 记账机构
       a.SA_RCRD_INSTN_NO,
       -- 客户编号
       a.SA_CUST_NO,
       -- 客户名称
       a.SA_CUST_NAME,
       -- 联系人编号
       a.SA_CONNTR_NO,
       -- 销户机构号
       a.SA_CACCT_INSTN_NO,
       -- 销户日期
       a.SA_CACCT_DT,
       -- 开户柜员号
       a.SA_OPAC_TL_NO,
       -- 销户柜员号
       a.SA_CACCT_TL_NO,
       -- 理财卡一户通备付金签约标志
       a.SA_YHT_SIGN_FLG,
       -- 校园卡标志
       a.SA_XYK_FLG,
       -- 支付限额控制标志
       a.SA_LMT_CTL_FLG,
       -- 卡号
       a.SA_CARD_NO,
       -- 综合理财标志
       a.SA_FS_STS,
       -- 是否贷记卡指定还款帐号
       a.SA_CCR_FLG,
       -- P9开始日期
       a.P9_START_DATE,
       -- P9结束日期
       a.P9_END_DATE
  FROM INN_TODDC_SAACNACN_H a
 WHERE LOAD_DATE='${log_date}';

-- 去重
INSERT OVERWRITE TABLE CT_TODDC_SAACNACN_H_MID PARTITION(DATA_TYPE='${log_date}_ALL')
SELECT 
       -- 帐号
       a.SA_ACCT_NO,
       -- 开户金额
       a.SA_OPAC_AMT,
       -- 开户日期
       a.SA_OPAC_DT,
       -- 记账机构
       a.SA_RCRD_INSTN_NO,
       -- 客户编号
       a.SA_CUST_NO,
       -- 客户名称
       a.SA_CUST_NAME,
       -- 联系人编号
       a.SA_CONNTR_NO,
       -- 销户机构号
       a.SA_CACCT_INSTN_NO,
       -- 销户日期
       a.SA_CACCT_DT,
       -- 开户柜员号
       a.SA_OPAC_TL_NO,
       -- 销户柜员号
       a.SA_CACCT_TL_NO,
       -- 理财卡一户通备付金签约标志
       a.SA_YHT_SIGN_FLG,
       -- 校园卡标志
       a.SA_XYK_FLG,
       -- 支付限额控制标志
       a.SA_LMT_CTL_FLG,
       -- 卡号
       a.SA_CARD_NO,
       -- 综合理财标志
       a.SA_FS_STS,
       -- 是否贷记卡指定还款帐号
       a.SA_CCR_FLG,
       -- P9开始日期
       a.P9_START_DATE,
       -- P9结束日期
       a.P9_END_DATE
  FROM (SELECT SA_ACCT_NO, SA_OPAC_AMT, SA_OPAC_DT, SA_RCRD_INSTN_NO,
               SA_CUST_NO, SA_CUST_NAME, SA_CONNTR_NO, SA_CACCT_INSTN_NO,
               SA_CACCT_DT, SA_OPAC_TL_NO, SA_CACCT_TL_NO, SA_YHT_SIGN_FLG,
               SA_XYK_FLG, SA_LMT_CTL_FLG, SA_CARD_NO, SA_FS_STS,
               SA_CCR_FLG, 
               P9_START_DATE, P9_END_DATE,
               row_number() over (
                    partition by SA_ACCT_NO, SA_OPAC_AMT, SA_OPAC_DT, SA_RCRD_INSTN_NO,
               SA_CUST_NO, SA_CUST_NAME, SA_CONNTR_NO, SA_CACCT_INSTN_NO,
               SA_CACCT_DT, SA_OPAC_TL_NO, SA_CACCT_TL_NO, SA_YHT_SIGN_FLG,
               SA_XYK_FLG, SA_LMT_CTL_FLG, SA_CARD_NO, SA_FS_STS,
               SA_CCR_FLG
                    order by P9_START_DATE
                   ) rownum
         FROM CT_TODDC_SAACNACN_H_MID 
        WHERE DATA_TYPE in ('${log_date}_INC', '${log_date_less_1}_ALL') 
        ) a
 WHERE a.rownum = 1;

-- Hive动态分区参数设置
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=strick;

-- 重建拉链
INSERT OVERWRITE TABLE CT_TODDC_SAACNACN_H PARTITION(P9_END_DATE)
SELECT SA_ACCT_NO, SA_OPAC_AMT, SA_OPAC_DT, SA_RCRD_INSTN_NO,
               SA_CUST_NO, SA_CUST_NAME, SA_CONNTR_NO, SA_CACCT_INSTN_NO,
               SA_CACCT_DT, SA_OPAC_TL_NO, SA_CACCT_TL_NO, SA_YHT_SIGN_FLG,
               SA_XYK_FLG, SA_LMT_CTL_FLG, SA_CARD_NO, SA_FS_STS,
               SA_CCR_FLG, 
       P9_START_DATE, 
       lead(P9_START_DATE, 1, '29991231') over (partition by SA_ACCT_NO order by P9_START_DATE) as P9_END_DATE
  FROM CT_TODDC_SAACNACN_H_MID
 WHERE DATA_TYPE='${log_date}_ALL';
