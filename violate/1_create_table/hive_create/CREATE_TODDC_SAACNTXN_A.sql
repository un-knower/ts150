use sor;

-- Hive建表脚本
-- 活期存款明细档.个人: TODDC_SAACNTXN_A

-- 外部表
DROP TABLE IF EXISTS EXT_TODDC_SAACNTXN_A;

CREATE EXTERNAL TABLE IF NOT EXISTS EXT_TODDC_SAACNTXN_A(
   -- 帐号
   SA_ACCT_NO                     string,
   -- （空值）
   SATXN_LL                       string,
   -- 活存帐户明细号
   SA_DDP_ACCT_NO_DET_N           string,
   -- （空值）
   SATXN_DB_TIMESTAMP             string,
   -- 币别
   SA_CURR_COD                    string,
   -- 操作员号
   SA_OPR_NO                      string,
   -- 钞汇鉴别
   SA_CURR_IDEN                   string,
   -- 冲正标志
   SA_EC_FLG                      string,
   -- 冲正明细号
   SA_EC_DET_NO                   string,
   -- 贷方发生额
   SA_CR_AMT                      string,
   -- 活存帐户余额
   SA_DDP_ACCT_BAL                string,
   -- 交易金额
   SA_TX_AMT                      string,
   -- 交易卡号
   SA_TX_CRD_NO                   string,
   -- 交易类别
   SA_TX_TYP                      string,
   -- 交易流水号
   SA_TX_LOG_NO                   string,
   -- 借方发生额
   SA_DR_AMT                      string,
   -- 凭证号码
   SA_DOC_NO                      string,
   -- 凭证种类
   SA_DOC_TYP                     string,
   -- 起息日
   SA_VAL_DT                      string,
   -- 手续费
   SA_SVC                         string,
   -- 授权号
   SA_AUTH_NO                     string,
   -- 商户交单套号
   SA_CUST_DOCAG_STNO             string,
   -- 营业单位代码
   SA_OPUN_COD                    string,
   -- 摘要代码
   SA_DSCRP_COD                   string,
   -- 备注
   SA_RMRK                        string,
   -- 交易时间
   SA_TX_TM                       string,
   -- 交易日期
   SA_TX_DT                       string,
   -- 系统日期
   SA_SYS_DT                      string,
   -- 活存积数
   SA_DDP_PDT                     string,
   -- 交易代码
   SA_APP_TX_CODE                 string,
   -- ETX档标志
   SA_EXT_FLG                     string,
   -- 境外消费明细扩展标志
   SA_OTX_FLG                     string,
   -- #FILLER
   FILLER                         string,
   -- 备注1
   SA_RMRK_1                      string,
   -- 对方户名
   SA_OP_CUST_NAME                string,
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
DROP TABLE IF EXISTS INN_TODDC_SAACNTXN_A;

CREATE TABLE IF NOT EXISTS INN_TODDC_SAACNTXN_A(
   -- 帐号
   SA_ACCT_NO                     string,
   -- （空值）
   SATXN_LL                       string,
   -- 活存帐户明细号
   SA_DDP_ACCT_NO_DET_N           string,
   -- （空值）
   SATXN_DB_TIMESTAMP             string,
   -- 币别
   SA_CURR_COD                    string,
   -- 操作员号
   SA_OPR_NO                      string,
   -- 钞汇鉴别
   SA_CURR_IDEN                   string,
   -- 冲正标志
   SA_EC_FLG                      string,
   -- 冲正明细号
   SA_EC_DET_NO                   string,
   -- 贷方发生额
   SA_CR_AMT                      string,
   -- 活存帐户余额
   SA_DDP_ACCT_BAL                string,
   -- 交易金额
   SA_TX_AMT                      string,
   -- 交易卡号
   SA_TX_CRD_NO                   string,
   -- 交易类别
   SA_TX_TYP                      string,
   -- 交易流水号
   SA_TX_LOG_NO                   string,
   -- 借方发生额
   SA_DR_AMT                      string,
   -- 凭证号码
   SA_DOC_NO                      string,
   -- 凭证种类
   SA_DOC_TYP                     string,
   -- 起息日
   SA_VAL_DT                      string,
   -- 手续费
   SA_SVC                         string,
   -- 授权号
   SA_AUTH_NO                     string,
   -- 商户交单套号
   SA_CUST_DOCAG_STNO             string,
   -- 营业单位代码
   SA_OPUN_COD                    string,
   -- 摘要代码
   SA_DSCRP_COD                   string,
   -- 备注
   SA_RMRK                        string,
   -- 交易时间
   SA_TX_TM                       string,
   -- 交易日期
   SA_TX_DT                       string,
   -- 系统日期
   SA_SYS_DT                      string,
   -- 活存积数
   SA_DDP_PDT                     string,
   -- 交易代码
   SA_APP_TX_CODE                 string,
   -- ETX档标志
   SA_EXT_FLG                     string,
   -- 境外消费明细扩展标志
   SA_OTX_FLG                     string,
   -- #FILLER
   FILLER                         string,
   -- 备注1
   SA_RMRK_1                      string,
   -- 对方户名
   SA_OP_CUST_NAME                string
)
PARTITIONED BY (P9_DATA_DATE string)
STORED AS ORC;

