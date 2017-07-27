use sor;

-- Hive贴源数据处理
-- ATM交易明细档: TODDC_CRATMDET_A

-- 指定新数据日期分区位置
ALTER TABLE EXT_TODDC_CRATMDET_A DROP IF EXISTS PARTITION(LOAD_DATE='${log_date}');
ALTER TABLE EXT_TODDC_CRATMDET_A ADD PARTITION (LOAD_DATE='${log_date}') LOCATION 'hdfs://hacluster/bigdata/input/TS150/case_trace/TODDC_CRATMDET_A/${log_date}/';

-- 备份贴源数据到ORC内部表
INSERT OVERWRITE TABLE INN_TODDC_CRATMDET_A PARTITION(P9_DATA_DATE='${log_date}')
SELECT 
   -- ATM编号
   CR_ATM_NO,
   -- (空值)
   CRDET_LL,
   -- 交易日期
   regexp_replace(CR_TX_DT, '-', ''),
   -- ATM交易序号
   CR_ATM_TX_SQ_NO,
   -- (空值)
   CRDET_DB_TIMESTAMP,
   -- 备用标志
   CR_RSV_FLG,
   -- 币别
   CR_CURR_COD,
   -- 冲正标志
   CR_EC_FLG,
   -- 冲正员号
   CR_OPR_EC,
   -- 记帐日期
   regexp_replace(CR_ENTR_DT, '-', ''),
   -- 交易代号
   CR_TX_NO,
   -- 交易金额
   CR_TX_AMT,
   -- 交易流水号
   CR_TX_LOG_NO,
   -- 交易时间
   CR_TX_TM,
   -- 交易网点号
   CR_TX_NETP_NO,
   -- 卡号
   CR_CRD_NO,
   -- 系统日期
   regexp_replace(CR_CPU_DT, '-', ''),
   -- 金卡交易流水号
   CR_GCRD_TX_SQ_NO,
   -- 龙卡交易流水号
   CR_DRGN_CRD_TX_SQ_N,
   -- 确认标志
   CR_CONF_FLG,
   -- 摘要代码
   CR_DSCRP_COD,
   -- 转入帐号
   CR_TRNI_SAV_NO,
   -- 转出帐号
   CR_TRNO_SAV_NO,
   -- 笔号
   CR_PSBK_SQ_NO,
   -- #FILLER
   FILLER
  FROM EXT_TODDC_CRATMDET_A
 WHERE LOAD_DATE='${log_date}';
