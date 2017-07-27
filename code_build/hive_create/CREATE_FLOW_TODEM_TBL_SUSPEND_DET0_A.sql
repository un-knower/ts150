use sor;

-- Hive建表脚本
-- 挂起交易明细表: TODEM_TBL_SUSPEND_DET0_A

-- 外部表
DROP TABLE IF EXISTS EXT_TODEM_TBL_SUSPEND_DET0_A;

CREATE EXTERNAL TABLE IF NOT EXISTS EXT_TODEM_TBL_SUSPEND_DET0_A(
    FLOW_NO                   string comment '交易流水号',
    CONFIRM_FLOW_NO           string comment '',
    BLACK_ID                  string comment '',
    CUST_NO                   string comment 'CCBS客户编号',
    CHANL_NO                  string comment '渠道编号',
    CUST_TYPE                 string comment '客户类型',
    CHANL_TRAD_NO             string comment '交易码',
    CHANL_ADAP                string comment '',
    ACCT_NO                   string comment '帐号',
    TRAD_DATE                 string comment '交易日期',
    TRAD_TIME                 string comment '交易时间',
    AMT1                      string comment '金额1',
    TRAD_BRAN                 string comment '交易机构',
    CONFIRM_SERVICE           string comment '',
    OTHER_SERVICE             string comment '',
    RULE_ID                   string comment '规则编号',
    RULE_DESC                 string comment '规则描述',
    OPT_ID                    string comment '选项编号',
    PROC_DATE                 string comment '处理日期',
    PROC_TIME                 string comment '',
    AP_STATUS                 string comment '',
    STATUS                    string comment '状态',
    DEAL_FLAG                 string comment '优化前后标志',
    P9_DATA_DATE              string comment 'P9数据日期',
    P9_BATCH_NUMBER           string comment 'P9批次号',
    P9_DEL_FLAG               string comment '',
    P9_DEL_DATE               string comment 'P9删除日期',
    P9_DEL_BATCH              string comment 'P9删除批次号',
    P9_JOB_NAME               string comment ''
)
PARTITIONED BY (LOAD_DATE string)
STORED AS TEXTFILE;

-- ORC内部表，节约存储空间
DROP TABLE IF EXISTS INN_TODEM_TBL_SUSPEND_DET0_A;

CREATE TABLE IF NOT EXISTS INN_TODEM_TBL_SUSPEND_DET0_A(
    FLOW_NO                   string comment '交易流水号',
    CONFIRM_FLOW_NO           string comment '',
    BLACK_ID                  string comment '',
    CUST_NO                   string comment 'CCBS客户编号',
    CHANL_NO                  string comment '渠道编号',
    CUST_TYPE                 string comment '客户类型',
    CHANL_TRAD_NO             string comment '交易码',
    CHANL_ADAP                string comment '',
    ACCT_NO                   string comment '帐号',
    TRAD_DATE                 string comment '交易日期',
    TRAD_TIME                 string comment '交易时间',
    AMT1                      string comment '金额1',
    TRAD_BRAN                 string comment '交易机构',
    CONFIRM_SERVICE           string comment '',
    OTHER_SERVICE             string comment '',
    RULE_ID                   string comment '规则编号',
    RULE_DESC                 string comment '规则描述',
    OPT_ID                    string comment '选项编号',
    PROC_DATE                 string comment '处理日期',
    PROC_TIME                 string comment '',
    AP_STATUS                 string comment '',
    STATUS                    string comment '状态',
    DEAL_FLAG                 string comment '优化前后标志'
)
PARTITIONED BY (P9_DATA_DATE string)
STORED AS ORC;
