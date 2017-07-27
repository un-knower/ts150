--输入输出数据
--IN_CUR_HDFS="/bigdata/input/TS150/case_trace/TODEC_LOGIN_TRAD_FLOW_A"
--OUT_CUR_HIVE="sor.INN_TODEC_LOGIN_TRAD_FLOW_A"

use sor;

-- Hive贴源数据处理
-- 登录流水表: TODEC_LOGIN_TRAD_FLOW_A

-- 指定新数据日期分区位置
ALTER TABLE EXT_TODEC_LOGIN_TRAD_FLOW_A DROP IF EXISTS PARTITION(LOAD_DATE='${log_date}');
ALTER TABLE EXT_TODEC_LOGIN_TRAD_FLOW_A ADD PARTITION (LOAD_DATE='${log_date}')
            LOCATION 'hdfs://hacluster/bigdata/input/TS150/case_trace/TODEC_LOGIN_TRAD_FLOW_A/${log_date}/';

-- 备份贴源数据到ORC内部表
INSERT OVERWRITE TABLE INN_TODEC_LOGIN_TRAD_FLOW_A PARTITION(P9_DATA_DATE='${log_date}')
SELECT
    QUERY_FLOW_NO,                       -- 平台查询流水号
    CHANL_NO,                            -- 渠道编号
    FLAT_TRAD_DATE,                      -- 交易日期
    CUST_NO,                             -- CCBS客户编号
    CHANL_CUST_NO,                       -- 渠道客户号
    CUST_NM,                             -- 户名
    ACCT_NO,                             -- 帐号
    ACCT_TYP,                            -- 帐别
    ACCT_BRAN,                           -- 主账号所属机构
    TRAD_NO,                             -- 平台交易码
    TRAD_NM,                             -- 平台交易名称
    ITCD_NO,                             -- 业务编码
    FLAT_TRAD_TIME,                      -- 平台交易时间
    TRAD_BRAN,                           -- 交易机构
    RETURN_CODE,                         -- 响应码
    RETURN_MSG,                          -- 响应信息
    MOBILE,                              -- 手机号
    IP,                                  -- IP地址
    TERM_QRY,                            -- 登录设备查询
    TERM_BIOS,                           -- PC机BIOS序列号
    TERM_IMEI,                           -- 手机标识
    TERM_MAC                             -- 网卡Mac地址
  FROM EXT_TODEC_LOGIN_TRAD_FLOW_A
 WHERE LOAD_DATE='${log_date}';
