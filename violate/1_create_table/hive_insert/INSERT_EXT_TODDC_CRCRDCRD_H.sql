use sor;

# Hive贴源数据处理
# 银行卡主档: TODDC_CRCRDCRD_H

-- 指定新数据日期分区位置
ALTER TABLE EXT_TODDC_CRCRDCRD_H DROP IF EXISTS PARTITION(LOAD_DATE='${log_date}');
ALTER TABLE EXT_TODDC_CRCRDCRD_H ADD PARTITION (LOAD_DATE='${log_date}') LOCATION 'hdfs://hacluster/bigdata/input/case_trace/TODDC_CRCRDCRD_H/${log_date}/';

-- 备份贴源数据到ORC内部表
INSERT OVERWRITE TABLE INN_TODDC_CRCRDCRD_H PARTITION(LOAD_DATE='${log_date}')
SELECT 
   -- (空值)
   CRCRD_LL,
   -- 卡号
   CR_CRD_NO,
   -- (空值)
   CRCRD_DB_TIMESTAMP,
   -- 备用标志1;员工标志
   CR_RSV_FLG_1,
   -- 备用标志2;金卡标志
   CR_RSV_FLG_2,
   -- 备用标志3;[信用/储蓄]卡标志
   CR_RSV_FLG_3,
   -- 备用标志4;带折标志
   CR_RSV_FLG_4,
   -- 备用标志5;综合理财标志
   CR_RSV_FLG_5,
   -- 备用标志6;发卡标志
   CR_RSV_FLG_6,
   -- 备用标志7;级别标志
   CR_RSV_FLG_7,
   -- 备用标志8
   CR_RSV_FLG_8,
   -- 彩照标志
   R_COLOR_PHOTO_FLAG,
   -- 单位卡标志
   CR_UNIT_CRD_FLG,
   -- 单位客户编号
   CR_UNIT_CUST_NO,
   -- 当日ATM取款次数
   CRNT_DT_ATM_DRW_TM,
   -- 当日ATM取款金额
   RNT_DT_ATM_DRW_AMT,
   -- 挂失日期
   regexp_replace(CR_DL_DT, '-', ''),
   -- 挂失状态
   CR_DL_STS,
   -- 借贷别
   CR_DR_CR_COD,
   -- 卡种代码
   CR_CRD_TYP_COD,
   -- 卡状态
   CR_CRD_STS,
   -- 客户编号
   CR_CUST_NO,
   -- 联名代码
   CR_JONT_COD,
   -- 注销状态一;银行主动注销
   CR_CNCL_STS1,
   -- 注销状态二;主卡注销附属卡
   CR_CNCL_STS2,
   -- 注销状态三;单位注销单位卡
   CR_CNCL_STS3,
   -- 注销状态四;担保人撤保
   CR_CNCL_STS4,
   -- 注销状态五;主卡申请注销
   CR_CNCL_STS5,
   -- 注销状态六;系统自动止付
   CR_CNCL_STS6,
   -- 认同代码
   CR_AGRE_COD,
   -- 事故序号
   CR_ACCD_SQ_NO,
   -- 收费标志1
   CR_COLC_FLG1,
   -- 收费标志2
   CR_COLC_FLG2,
   -- 收费标志3
   CR_COLC_FLG3,
   -- 收费标志4
   CR_COLC_FLG4,
   -- 收费标志5
   CR_COLC_FLG5,
   -- 收费标志6
   CR_COLC_FLG6,
   -- 收费标志7;临时额度标志
   CR_COLC_FLG7,
   -- 收费标志8;银联标志
   CR_COLC_FLG8,
   -- 信用等级
   CR_CRDG,
   -- 延期状态
   CR_DELAY_STS,
   -- 年费收取比例
   CR_ANFE_CHG_PCT,
   -- 年费已扣收金额
   CR_ANFE_REV_AMT,
   -- 年费收取年份
   CR_ANFE_YEAR,
   -- 年费未扣收月数
   ANFE_UNPAY_MONTHS,
   -- CVV2
   CR_CVV2,
   -- 备注
   CR_FILLER_CRD,
   -- 营业单位代码
   CR_OPUN_COD,
   -- 有效期限
   CR_EXPD_DT,
   -- 制卡状态
   CR_CRDMD_STS,
   -- 主附卡标志
   CR_MNSUB_DRP,
   -- 主卡卡号
   CR_PCRD_NO,
   -- 注销日期
   regexp_replace(CR_CNCLC_DT, '-', ''),
   -- 注销状态
   CR_CNCL_STS,
   -- 专用代码
   CR_SPUS_COD,
   -- 版本号
   CR_VRSN_NO,
   -- 卡片工本费收取比例
   R_CRD_COST_REV_RTO,
   -- 代发工资标志
   CR_AGENT_PAY_FLG,
   -- ATM上次交易日期
   regexp_replace(CR_ATM_LTM_TX_DT, '-', ''),
   -- 客户申请编号
   CR_CUST_APLY_NO,
   -- 更改日期
   regexp_replace(CR_CHG_GRNTR_DT, '-', ''),
   -- 开卡日期
   regexp_replace(CR_OPCR_DATE, '-', ''),
   -- 当日ATM转帐次数
   CRNT_DT_ATM_TRN_TM,
   -- 当日ATM转帐金额
   RNT_DT_ATM_TRN_AMT,
   -- 当日他行转帐次数
   CRNT_DT_TB_TRN_TM,
   -- 当日他行转帐金额
   CRNT_DT_TB_TRN_AMT,
   -- 止付日期
   regexp_replace(CR_STPMT_DATE, '-', ''),
   -- 发卡机构号
   CR_APP_NO,
   -- 当日外币ATM取款次数
   CRNT_FX_ATM_DRW_TM,
   -- 当日外币ATM取款金额
   RNT_FX_AMT_DRW_AMT,
   -- 密码
   CR_PSWD,
   -- 密码次数
   CR_PSWD_CNT,
   -- 二磁道
   CR_SEC_MT_CON,
   -- 二磁道校验次数
   CR_SEC_MT_CON_CNT,
   -- POS密码检查标志
   CR_POS_CHK_FLG,
   -- IC卡主帐户
   CR_IC_PCRD_NO,
   -- 换卡日期
   regexp_replace(CR_CRD_CHG_DT, '-', ''),
   -- IC卡数量
   CR_IC_CNT,
   -- 换卡机构号
   CR_CHG_OPUN_COD,
   -- 失效日期出错次数
   HK_INVALI_DT_TIMES,
   -- #FILLER
   FILLER,
   -- P9开始日期
   regexp_replace(P9_START_DATE, '-', ''),
   -- P9结束日期
   regexp_replace(P9_END_DATE, '-', '')
  FROM EXT_TODDC_CRCRDCRD_H
 WHERE LOAD_DATE='${log_date}';
