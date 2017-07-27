use sor;

-- Hive贴源数据处理
-- 排队机流水: T0162_CUST_QUEUE_INFO_A

-- 指定新数据日期分区位置
ALTER TABLE EXT_T0162_CUST_QUEUE_INFO_A DROP IF EXISTS PARTITION(LOAD_DATE='${log_date}');
ALTER TABLE EXT_T0162_CUST_QUEUE_INFO_A ADD PARTITION (LOAD_DATE='${log_date}') LOCATION 'hdfs://hacluster/bigdata/input/TS150/case_trace/T0162_CUST_QUEUE_INFO_A/${log_date}/';

-- 备份贴源数据到ORC内部表
INSERT OVERWRITE TABLE INN_T0162_CUST_QUEUE_INFO_A PARTITION(P9_DATA_DATE='${log_date}')
SELECT 
   -- 机构代码
   CCBINS_ID             ,
   -- 
   regexp_replace(CST_OFRNUM_DT         , '-', ''),
   -- 
   CST_OFRNUM_TM         ,
   -- 
   TLR_CLNUM_TM          ,
   -- 
   TLR_SVC_EDTM          ,
   -- 
   CST_ASS_TM            ,
   -- 
   BO_QU_BILLNO          ,
   -- 
   SRCSYS_AR_ID          ,
   -- 
   BO_QU_OFRNUM_BSN_CTCD ,
   -- 
   LVL2_BSN_CTCD         ,
   -- 
   CST_QU_STCD           ,
   -- 
   CST_TPCD              ,
   -- 
   MSG_EMAIL_PRTY        ,
   -- 
   CST_SSF_CD            ,
   -- 
   BO_CNTER_ID           ,
   -- 
   ORIG_STM_USR_ID       ,
   -- 
   REQ_CCB_PDRCMM_IND    ,
   -- 
   CMPN_CHNL_CD          ,
   -- 
   RCMCST_MGR_ID         ,
   -- 
   RSRV_FLD1_INF         
  FROM EXT_T0162_CUST_QUEUE_INFO_A
 WHERE LOAD_DATE='${log_date}';
