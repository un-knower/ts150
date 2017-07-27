use sor;

-- Hive建表脚本
-- 个人活期存款明细扩展档: TODDC_SAETXETX_A

-- 外部表
DROP TABLE IF EXISTS EXT_TODDC_SAETXETX_A;

CREATE EXTERNAL TABLE IF NOT EXISTS EXT_TODDC_SAETXETX_A(
   -- 记录长度
   SAETX_LL                       string,
   -- 帐号
   SA_ACCT_NO                     string,
   -- 活存帐户明细号
   SA_DDP_ACCT_NO_DET_N           string,
   -- 时间戳
   SAETX_DB_TIMESTAMP             string,
   -- 对方帐号
   SA_OP_ACCT_NO_32               string,
   -- 对方户名
   SA_OP_CUST_NAME                string,
   -- 备注
   SA_RMRK                        string,
   -- 操作员号
   SA_OPR_NO                      string,
   -- 授权员号
   SA_SPV_NO                      string,
   -- 对方行号
   SA_OP_BANK_NO                  string,
   -- 外系统支付备注
   SA_B2B_B2C_RMRK                string,
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
DROP TABLE IF EXISTS INN_TODDC_SAETXETX_A;

CREATE TABLE IF NOT EXISTS INN_TODDC_SAETXETX_A(
   -- 记录长度
   SAETX_LL                       string,
   -- 帐号
   SA_ACCT_NO                     string,
   -- 活存帐户明细号
   SA_DDP_ACCT_NO_DET_N           string,
   -- 时间戳
   SAETX_DB_TIMESTAMP             string,
   -- 对方帐号
   SA_OP_ACCT_NO_32               string,
   -- 对方户名
   SA_OP_CUST_NAME                string,
   -- 备注
   SA_RMRK                        string,
   -- 操作员号
   SA_OPR_NO                      string,
   -- 授权员号
   SA_SPV_NO                      string,
   -- 对方行号
   SA_OP_BANK_NO                  string,
   -- 外系统支付备注
   SA_B2B_B2C_RMRK                string,
   -- #FILLER
   FILLER                         string
)
PARTITIONED BY (P9_DATA_DATE string)
STORED AS ORC;

