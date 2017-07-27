use sor;

-- Hive贴源数据处理
-- 个人活期存款明细扩展档: TODDC_SAETXETX_A

-- 指定新数据日期分区位置
ALTER TABLE EXT_TODDC_SAETXETX_A DROP IF EXISTS PARTITION(LOAD_DATE='${log_date}');
ALTER TABLE EXT_TODDC_SAETXETX_A ADD PARTITION (LOAD_DATE='${log_date}') LOCATION 'hdfs://hacluster/bigdata/input/TS150/case_trace/TODDC_SAETXETX_A/${log_date}/';

-- 备份贴源数据到ORC内部表
INSERT OVERWRITE TABLE INN_TODDC_SAETXETX_A PARTITION(P9_DATA_DATE='${log_date}')
SELECT 
   -- 记录长度
   SAETX_LL,
   -- 帐号
   SA_ACCT_NO,
   -- 活存帐户明细号
   SA_DDP_ACCT_NO_DET_N,
   -- 时间戳
   SAETX_DB_TIMESTAMP,
   -- 对方帐号
   SA_OP_ACCT_NO_32,
   -- 对方户名
   SA_OP_CUST_NAME,
   -- 备注
   SA_RMRK,
   -- 操作员号
   SA_OPR_NO,
   -- 授权员号
   SA_SPV_NO,
   -- 对方行号
   SA_OP_BANK_NO,
   -- 外系统支付备注
   SA_B2B_B2C_RMRK,
   -- #FILLER
   FILLER
  FROM EXT_TODDC_SAETXETX_A
 WHERE LOAD_DATE='${log_date}';
