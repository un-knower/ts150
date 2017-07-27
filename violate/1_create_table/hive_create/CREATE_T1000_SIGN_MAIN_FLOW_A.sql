use sor;

-- Hive建表脚本
-- 签约主流水表: T1000_SIGN_MAIN_FLOW_A

-- 外部表
DROP TABLE IF EXISTS EXT_T1000_SIGN_MAIN_FLOW_A;

CREATE EXTERNAL TABLE IF NOT EXISTS EXT_T1000_SIGN_MAIN_FLOW_A(
   -- 交易流水号
   FLOW_NO                        string,
   -- 流水号
   CHANL_FLOW_NO                  string,
   -- ecif流水号
   ECIF_FLOW_NO                   string,
   -- ECIF客户编号
   ECIF_CUST_NO                   string,
   -- CCBS客户编号
   CUST_NO                        string,
   -- 渠道客户号
   CHANL_CUST_NO                  string,
   -- 证件类型
   CERT_TYP                       string,
   -- 证件号码
   CERT_ID                        string,
   -- 户名
   CUST_NM                        string,
   -- 交易码
   CHANL_TRAD_NO                  string,
   -- 渠道编号
   CHANL_NO                       string,
   -- 签约途径
   SIGN_APPR                      string,
   -- 签约机构
   SIGN_BRAN                      string,
   -- 开通签约服务柜员
   SIGN_TLR                       string,
   -- 签约日期
   SIGN_DATE                      string,
   -- 交易时间
   SIGN_TIME                      string,
   -- #终端信息
   TERM_INF                       string,
   -- 成功标志
   TRAD_STS                       string,
   -- 响应码
   RETURN_COD                     string,
   -- 响应信息
   RETURN_MSG                     string,
   -- 说明1
   REMARK1                        string,
   -- 说明2
   REMARK2                        string,
   -- 备注3
   REMARK3                        string,
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
DROP TABLE IF EXISTS INN_T1000_SIGN_MAIN_FLOW_A;

CREATE TABLE IF NOT EXISTS INN_T1000_SIGN_MAIN_FLOW_A(
   -- 交易流水号
   FLOW_NO                        string,
   -- 流水号
   CHANL_FLOW_NO                  string,
   -- ecif流水号
   ECIF_FLOW_NO                   string,
   -- ECIF客户编号
   ECIF_CUST_NO                   string,
   -- CCBS客户编号
   CUST_NO                        string,
   -- 渠道客户号
   CHANL_CUST_NO                  string,
   -- 证件类型
   CERT_TYP                       string,
   -- 证件号码
   CERT_ID                        string,
   -- 户名
   CUST_NM                        string,
   -- 交易码
   CHANL_TRAD_NO                  string,
   -- 渠道编号
   CHANL_NO                       string,
   -- 签约途径
   SIGN_APPR                      string,
   -- 签约机构
   SIGN_BRAN                      string,
   -- 开通签约服务柜员
   SIGN_TLR                       string,
   -- 签约日期
   SIGN_DATE                      string,
   -- 交易时间
   SIGN_TIME                      string,
   -- #终端信息
   TERM_INF                       string,
   -- 成功标志
   TRAD_STS                       string,
   -- 响应码
   RETURN_COD                     string,
   -- 响应信息
   RETURN_MSG                     string,
   -- 说明1
   REMARK1                        string,
   -- 说明2
   REMARK2                        string,
   -- 备注3
   REMARK3                        string
)
PARTITIONED BY (P9_DATA_DATE string)
STORED AS ORC;

