use sor;

-- Hive建表脚本
-- 排队机流水: T0162_CUST_QUEUE_INFO_A

-- 外部表
DROP TABLE IF EXISTS EXT_T0162_CUST_QUEUE_INFO_A;

CREATE EXTERNAL TABLE IF NOT EXISTS EXT_T0162_CUST_QUEUE_INFO_A(
   -- 机构代码
   CCBINS_ID                      string,
   -- 
   CST_OFRNUM_DT                  string,
   -- 
   CST_OFRNUM_TM                  string,
   -- 
   TLR_CLNUM_TM                   string,
   -- 
   TLR_SVC_EDTM                   string,
   -- 
   CST_ASS_TM                     string,
   -- 
   BO_QU_BILLNO                   string,
   -- 
   SRCSYS_AR_ID                   string,
   -- 
   BO_QU_OFRNUM_BSN_CTCD          string,
   -- 
   LVL2_BSN_CTCD                  string,
   -- 
   CST_QU_STCD                    string,
   -- 
   CST_TPCD                       string,
   -- 
   MSG_EMAIL_PRTY                 string,
   -- 
   CST_SSF_CD                     string,
   -- 
   BO_CNTER_ID                    string,
   -- 
   ORIG_STM_USR_ID                string,
   -- 
   REQ_CCB_PDRCMM_IND             string,
   -- 
   CMPN_CHNL_CD                   string,
   -- 
   RCMCST_MGR_ID                  string,
   -- 
   RSRV_FLD1_INF                  string,
   -- P9数据日期
   P9_DATA_DATE                   string,
   -- P9批次号
   P9_BATCH_NUMBER                string,
   -- P9删除标志
   P9_DEL_FLAG                    string,
   -- P9删除日期
   P9_DEL_DATE                    string,
   -- P9删除批次号
   P9_DEL_BATCH                   string,
   -- P9作业名
   P9_JOB_NAME                    string
)
PARTITIONED BY (LOAD_DATE string)
STORED AS TEXTFILE;

-- ORC内部表，节约存储空间
DROP TABLE IF EXISTS INN_T0162_CUST_QUEUE_INFO_A;

CREATE TABLE IF NOT EXISTS INN_T0162_CUST_QUEUE_INFO_A(
   -- 机构代码
   CCBINS_ID                      string,
   -- 
   CST_OFRNUM_DT                  string,
   -- 
   CST_OFRNUM_TM                  string,
   -- 
   TLR_CLNUM_TM                   string,
   -- 
   TLR_SVC_EDTM                   string,
   -- 
   CST_ASS_TM                     string,
   -- 
   BO_QU_BILLNO                   string,
   -- 
   SRCSYS_AR_ID                   string,
   -- 
   BO_QU_OFRNUM_BSN_CTCD          string,
   -- 
   LVL2_BSN_CTCD                  string,
   -- 
   CST_QU_STCD                    string,
   -- 
   CST_TPCD                       string,
   -- 
   MSG_EMAIL_PRTY                 string,
   -- 
   CST_SSF_CD                     string,
   -- 
   BO_CNTER_ID                    string,
   -- 
   ORIG_STM_USR_ID                string,
   -- 
   REQ_CCB_PDRCMM_IND             string,
   -- 
   CMPN_CHNL_CD                   string,
   -- 
   RCMCST_MGR_ID                  string,
   -- 
   RSRV_FLD1_INF                  string
)
PARTITIONED BY (P9_DATA_DATE string)
STORED AS ORC;

