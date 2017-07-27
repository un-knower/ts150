use sor;

-- Hive贴源数据处理
-- 活期存款明细档.个人: TODDC_SAACNTXN_A

-- 指定新数据日期分区位置
ALTER TABLE EXT_TODDC_SAACNTXN_A DROP IF EXISTS PARTITION(LOAD_DATE='${log_date}');
ALTER TABLE EXT_TODDC_SAACNTXN_A ADD PARTITION (LOAD_DATE='${log_date}') LOCATION 'hdfs://hacluster/bigdata/input/TS150/case_trace/TODDC_SAACNTXN_A/${log_date}/';

-- 备份贴源数据到ORC内部表
INSERT OVERWRITE TABLE INN_TODDC_SAACNTXN_A PARTITION(P9_DATA_DATE='${log_date}')
SELECT 
   -- 帐号
   SA_ACCT_NO,
   -- （空值）
   SATXN_LL,
   -- 活存帐户明细号
   SA_DDP_ACCT_NO_DET_N,
   -- （空值）
   SATXN_DB_TIMESTAMP,
   -- 币别
   SA_CURR_COD,
   -- 操作员号
   SA_OPR_NO,
   -- 钞汇鉴别
   SA_CURR_IDEN,
   -- 冲正标志
   SA_EC_FLG,
   -- 冲正明细号
   SA_EC_DET_NO,
   -- 贷方发生额
   SA_CR_AMT,
   -- 活存帐户余额
   SA_DDP_ACCT_BAL,
   -- 交易金额
   SA_TX_AMT,
   -- 交易卡号
   SA_TX_CRD_NO,
   -- 交易类别
   SA_TX_TYP,
   -- 交易流水号
   SA_TX_LOG_NO,
   -- 借方发生额
   SA_DR_AMT,
   -- 凭证号码
   SA_DOC_NO,
   -- 凭证种类
   SA_DOC_TYP,
   -- 起息日
   regexp_replace(SA_VAL_DT, '-', ''),
   -- 手续费
   SA_SVC,
   -- 授权号
   SA_AUTH_NO,
   -- 商户交单套号
   SA_CUST_DOCAG_STNO,
   -- 营业单位代码
   SA_OPUN_COD,
   -- 摘要代码
   SA_DSCRP_COD,
   -- 备注
   SA_RMRK,
   -- 交易时间
   SA_TX_TM,
   -- 交易日期
   regexp_replace(SA_TX_DT, '-', ''),
   -- 系统日期
   regexp_replace(SA_SYS_DT, '-', ''),
   -- 活存积数
   SA_DDP_PDT,
   -- 交易代码
   SA_APP_TX_CODE,
   -- ETX档标志
   SA_EXT_FLG,
   -- 境外消费明细扩展标志
   SA_OTX_FLG,
   -- #FILLER
   FILLER,
   -- 备注1
   SA_RMRK_1,
   -- 对方户名
   SA_OP_CUST_NAME
  FROM EXT_TODDC_SAACNTXN_A
 WHERE LOAD_DATE='${log_date}';
