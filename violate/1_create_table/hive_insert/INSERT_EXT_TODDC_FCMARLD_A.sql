use sor;

-- Hive贴源数据处理
-- 重要非帐务性流水: TODDC_FCMARLD_A

-- 指定新数据日期分区位置
ALTER TABLE EXT_TODDC_FCMARLD_A DROP IF EXISTS PARTITION(LOAD_DATE='${log_date}');
ALTER TABLE EXT_TODDC_FCMARLD_A ADD PARTITION (LOAD_DATE='${log_date}') LOCATION 'hdfs://hacluster/bigdata/input/TS150/case_trace/TODDC_FCMARLD_A/${log_date}/';

-- 备份贴源数据到ORC内部表
INSERT OVERWRITE TABLE INN_TODDC_FCMARLD_A PARTITION(P9_DATA_DATE='${log_date}')
SELECT 
   -- 重要非帐务流水序号
   CM_ARL_LOG_NO,
   -- 时间戳
   CMARL_DB_TIMESTAMP,
   -- 交易码
   CM_TX_ID,
   -- 终端号
   TERMINAL_ID,
   -- 备注1
   CM_LCL_OR_RSN,
   -- 备注2
   CM_HST_OR_RSN,
   -- 授权A级柜员号
   CM_SPV_A,
   -- 授权B级柜员号
   CM_SPV_B,
   -- 操作柜员号
   CM_TELLER_ID,
   -- 复核柜员号
   CM_VERIFY_TELLER,
   -- 重要非账务性流水标志
   CM_ANACT_SQ_IM_NO_FLAG,
   -- 交易日期
   regexp_replace(CM_TX_DT, '-', ''),
   -- 交易时间
   CM_TX_TM_R,
   -- 前端操作码
   CM_FRT_OPR_NO,
   -- 帐号
   CM_ACCT_NO,
   -- 交易自定义字段
   CM_APLY_DATA_DSCRP_ARL,
   -- #FILLER
   FILLER
  FROM EXT_TODDC_FCMARLD_A
 WHERE LOAD_DATE='${log_date}';
