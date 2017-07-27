use sor;

-- Hive建表脚本
-- 重要非帐务性流水: TODDC_FCMARLD_A

-- 外部表
DROP TABLE IF EXISTS EXT_TODDC_FCMARLD_A;

CREATE EXTERNAL TABLE IF NOT EXISTS EXT_TODDC_FCMARLD_A(
   -- 重要非帐务流水序号
   CM_ARL_LOG_NO                  string,
   -- 时间戳
   CMARL_DB_TIMESTAMP             string,
   -- 交易码
   CM_TX_ID                       string,
   -- 终端号
   TERMINAL_ID                    string,
   -- 备注1
   CM_LCL_OR_RSN                  string,
   -- 备注2
   CM_HST_OR_RSN                  string,
   -- 授权A级柜员号
   CM_SPV_A                       string,
   -- 授权B级柜员号
   CM_SPV_B                       string,
   -- 操作柜员号
   CM_TELLER_ID                   string,
   -- 复核柜员号
   CM_VERIFY_TELLER               string,
   -- 重要非账务性流水标志
   CM_ANACT_SQ_IM_NO_FLAG         string,
   -- 交易日期
   CM_TX_DT                       string,
   -- 交易时间
   CM_TX_TM_R                     string,
   -- 前端操作码
   CM_FRT_OPR_NO                  string,
   -- 帐号
   CM_ACCT_NO                     string,
   -- 交易自定义字段
   CM_APLY_DATA_DSCRP_ARL         string,
   -- #FILLER
   FILLER                         string,
   -- 按机构拆分字段
   P9_SPLIT_BRANCH_CD             string,
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
DROP TABLE IF EXISTS INN_TODDC_FCMARLD_A;

CREATE TABLE IF NOT EXISTS INN_TODDC_FCMARLD_A(
   -- 重要非帐务流水序号
   CM_ARL_LOG_NO                  string,
   -- 时间戳
   CMARL_DB_TIMESTAMP             string,
   -- 交易码
   CM_TX_ID                       string,
   -- 终端号
   TERMINAL_ID                    string,
   -- 备注1
   CM_LCL_OR_RSN                  string,
   -- 备注2
   CM_HST_OR_RSN                  string,
   -- 授权A级柜员号
   CM_SPV_A                       string,
   -- 授权B级柜员号
   CM_SPV_B                       string,
   -- 操作柜员号
   CM_TELLER_ID                   string,
   -- 复核柜员号
   CM_VERIFY_TELLER               string,
   -- 重要非账务性流水标志
   CM_ANACT_SQ_IM_NO_FLAG         string,
   -- 交易日期
   CM_TX_DT                       string,
   -- 交易时间
   CM_TX_TM_R                     string,
   -- 前端操作码
   CM_FRT_OPR_NO                  string,
   -- 帐号
   CM_ACCT_NO                     string,
   -- 交易自定义字段
   CM_APLY_DATA_DSCRP_ARL         string,
   -- #FILLER
   FILLER                         string
)
PARTITIONED BY (P9_DATA_DATE string)
STORED AS ORC;

