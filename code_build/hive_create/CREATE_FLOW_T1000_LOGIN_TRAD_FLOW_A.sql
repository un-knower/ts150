use sor;

-- Hive建表脚本
-- 登录痕迹: T1000_LOGIN_TRAD_FLOW_A

-- 外部表
DROP TABLE IF EXISTS EXT_T1000_LOGIN_TRAD_FLOW_A;

CREATE EXTERNAL TABLE IF NOT EXISTS EXT_T1000_LOGIN_TRAD_FLOW_A(
    PLAT_FLOW_NO              string comment '电子渠道平台流水号',
    CHNL_TPCD                 string comment '渠道类型代码',
    CHNL_ADAP_ID              string comment '渠道适配号',
    PLAT_TXN_DATE             string comment '平台交易日期',
    BACK_SYS_NM               string comment '后台系统名称',
    EBNK_CST_PLTFRM_ID        string comment '电子银行客户平台编号',
    CHNL_CUST_NO              string comment '渠道客户号',
    CST_NM                    string comment '客户名称',
    RSRV_TPCD                 string comment '备用类型代码',
    EBNK_LVL_ECD              string comment 'EBNK_LVL_ECD',
    EBNK_SIGN_ST              string comment '电子银行签约状态',
    ACCNO                     string comment '账号',
    ACC_TPCD                  string comment '账户类型代码',
    HDL_INSID                 string comment '经办机构编号',
    PLAT_TXN_CODE             string comment '电子渠道平台交易码',
    TXN_DSC                   string comment '交易描述',
    RSRV_2                    string comment '备用二',
    PLAT_TXN_TIME             string comment '平台交易时间',
    PLAT_SYS_DATE             string comment '平台系统日期',
    TXN_INSID                 string comment '交易机构编号',
    SIGN_INSID                string comment '签约机构编号',
    SYS_RESP_CODE             string comment '响应码',
    SYS_RESP_DESC             string comment '服务响应描述',
    TERM_INF                  string comment '终端信息',
    IP_ADD                    string comment 'IP地址',
    RSRV_1                    string comment '备用一',
    ECHNL_TXN_STCD            string comment '电子渠道交易状态代码',
    P9_DATA_DATE              string comment 'P9数据日期',
    P9_BATCH_NUMBER           string comment '',
    P9_DEL_FLAG               string comment '',
    P9_DEL_DATE               string comment '',
    P9_DEL_BATCH              string comment '',
    P9_JOB_NAME               string comment ''
)
PARTITIONED BY (LOAD_DATE string)
STORED AS TEXTFILE;

-- ORC内部表，节约存储空间
DROP TABLE IF EXISTS INN_T1000_LOGIN_TRAD_FLOW_A;

CREATE TABLE IF NOT EXISTS INN_T1000_LOGIN_TRAD_FLOW_A(
    MOBILE                    string comment '手机号',
    IP                        string comment 'IP地址'
)
PARTITIONED BY (P9_DATA_DATE string)
STORED AS ORC;
