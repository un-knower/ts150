use sor;

-- Hive建表脚本
-- 登录流水表: TODEC_LOGIN_TRAD_FLOW_A

-- 外部表
DROP TABLE IF EXISTS EXT_TODEC_LOGIN_TRAD_FLOW_A;

CREATE EXTERNAL TABLE IF NOT EXISTS EXT_TODEC_LOGIN_TRAD_FLOW_A(
    QUERY_FLOW_NO             string comment '平台查询流水号',
    CHANL_NO                  string comment '渠道编号',
    CHANL_ADAP_NO             string comment '渠道适配号',
    FLAT_TRAD_DATE            string comment '交易日期',
    BACK_TRAD_NM              string comment '后台系统名称',
    CUST_NO                   string comment 'CCBS客户编号',
    CHANL_CUST_NO             string comment '渠道客户号',
    CUST_NM                   string comment '户名',
    CUST_TYP                  string comment '客户类型',
    CUST_GRD                  string comment '客户级别',
    SIGN_STS                  string comment '签约状态',
    ACCT_NO                   string comment '帐号',
    ACCT_TYP                  string comment '帐别',
    ACCT_BRAN                 string comment '主账号所属机构',
    TRAD_NO                   string comment '平台交易码',
    TRAD_NM                   string comment '平台交易名称',
    ITCD_NO                   string comment '业务编码',
    FLAT_TRAD_TIME            string comment '平台交易时间',
    FLAT_SYS_DATE             string comment '平台系统日期',
    TRAD_BRAN                 string comment '交易机构',
    SIGN_BRAN                 string comment '签约机构',
    RETURN_CODE               string comment '响应码',
    RETURN_MSG                string comment '响应信息',
    TERM_INF                  string comment '终端信息',
    ADD_INF                   string comment '登录补充信息',
    MON_INF                   string comment '风险监控信息',
    TRAD_STS                  string comment '成功标志',
    P9_SPLIT_BRANCH_CD        string comment '按机构拆分字段',
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
DROP TABLE IF EXISTS INN_TODEC_LOGIN_TRAD_FLOW_A;

CREATE TABLE IF NOT EXISTS INN_TODEC_LOGIN_TRAD_FLOW_A(
    QUERY_FLOW_NO             string comment '平台查询流水号',
    CHANL_NO                  string comment '渠道编号',
    FLAT_TRAD_DATE            string comment '交易日期',
    CUST_NO                   string comment 'CCBS客户编号',
    CHANL_CUST_NO             string comment '渠道客户号',
    CUST_NM                   string comment '户名',
    ACCT_NO                   string comment '帐号',
    ACCT_TYP                  string comment '帐别',
    ACCT_BRAN                 string comment '主账号所属机构',
    TRAD_NO                   string comment '平台交易码',
    TRAD_NM                   string comment '平台交易名称',
    ITCD_NO                   string comment '业务编码',
    FLAT_TRAD_TIME            string comment '平台交易时间',
    TRAD_BRAN                 string comment '交易机构',
    RETURN_CODE               string comment '响应码',
    RETURN_MSG                string comment '响应信息',
    MOBILE                    string comment '手机号',
    IP                        string comment 'IP地址',
    TERM_QRY                  string comment '登录设备查询',
    TERM_BIOS                 string comment 'PC机BIOS序列号',
    TERM_IMEI                 string comment '手机标识',
    TERM_MAC                  string comment '网卡Mac地址'
)
PARTITIONED BY (P9_DATA_DATE string)
STORED AS ORC;
