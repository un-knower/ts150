use sor;

-- Hive建表脚本
-- ATM交易明细档: TODDC_CRATMDET_A

-- 外部表
DROP TABLE IF EXISTS EXT_TODDC_CRATMDET_A;

CREATE EXTERNAL TABLE IF NOT EXISTS EXT_TODDC_CRATMDET_A(
   -- ATM编号
   CR_ATM_NO                      string,
   -- (空值)
   CRDET_LL                       string,
   -- 交易日期
   CR_TX_DT                       string,
   -- ATM交易序号
   CR_ATM_TX_SQ_NO                string,
   -- (空值)
   CRDET_DB_TIMESTAMP             string,
   -- 备用标志
   CR_RSV_FLG                     string,
   -- 币别
   CR_CURR_COD                    string,
   -- 冲正标志
   CR_EC_FLG                      string,
   -- 冲正员号
   CR_OPR_EC                      string,
   -- 记帐日期
   CR_ENTR_DT                     string,
   -- 交易代号
   CR_TX_NO                       string,
   -- 交易金额
   CR_TX_AMT                      string,
   -- 交易流水号
   CR_TX_LOG_NO                   string,
   -- 交易时间
   CR_TX_TM                       string,
   -- 交易网点号
   CR_TX_NETP_NO                  string,
   -- 卡号
   CR_CRD_NO                      string,
   -- 系统日期
   CR_CPU_DT                      string,
   -- 金卡交易流水号
   CR_GCRD_TX_SQ_NO               string,
   -- 龙卡交易流水号
   CR_DRGN_CRD_TX_SQ_N            string,
   -- 确认标志
   CR_CONF_FLG                    string,
   -- 摘要代码
   CR_DSCRP_COD                   string,
   -- 转入帐号
   CR_TRNI_SAV_NO                 string,
   -- 转出帐号
   CR_TRNO_SAV_NO                 string,
   -- 笔号
   CR_PSBK_SQ_NO                  string,
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
DROP TABLE IF EXISTS INN_TODDC_CRATMDET_A;

CREATE TABLE IF NOT EXISTS INN_TODDC_CRATMDET_A(
   -- ATM编号
   CR_ATM_NO                      string,
   -- (空值)
   CRDET_LL                       string,
   -- 交易日期
   CR_TX_DT                       string,
   -- ATM交易序号
   CR_ATM_TX_SQ_NO                string,
   -- (空值)
   CRDET_DB_TIMESTAMP             string,
   -- 备用标志
   CR_RSV_FLG                     string,
   -- 币别
   CR_CURR_COD                    string,
   -- 冲正标志
   CR_EC_FLG                      string,
   -- 冲正员号
   CR_OPR_EC                      string,
   -- 记帐日期
   CR_ENTR_DT                     string,
   -- 交易代号
   CR_TX_NO                       string,
   -- 交易金额
   CR_TX_AMT                      string,
   -- 交易流水号
   CR_TX_LOG_NO                   string,
   -- 交易时间
   CR_TX_TM                       string,
   -- 交易网点号
   CR_TX_NETP_NO                  string,
   -- 卡号
   CR_CRD_NO                      string,
   -- 系统日期
   CR_CPU_DT                      string,
   -- 金卡交易流水号
   CR_GCRD_TX_SQ_NO               string,
   -- 龙卡交易流水号
   CR_DRGN_CRD_TX_SQ_N            string,
   -- 确认标志
   CR_CONF_FLG                    string,
   -- 摘要代码
   CR_DSCRP_COD                   string,
   -- 转入帐号
   CR_TRNI_SAV_NO                 string,
   -- 转出帐号
   CR_TRNO_SAV_NO                 string,
   -- 笔号
   CR_PSBK_SQ_NO                  string,
   -- #FILLER
   FILLER                         string
)
PARTITIONED BY (P9_DATA_DATE string)
STORED AS ORC;

