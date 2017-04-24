use sor;

-- Hive建表脚本
-- 综合前置客户查询流水: P2LOG_CCBS_RES

-- 外部表
DROP TABLE IF EXISTS EXT_P2LOG_CCBS_RES;

CREATE EXTERNAL TABLE IF NOT EXISTS EXT_P2LOG_CCBS_RES(
   -- 查询日期
   QUE_DATE                       string,
   -- 查询时间
   QUE_TIME                       string,
   -- 柜员号
   TLR_ID                         string,
   -- 
   APP_NO                         string,
   -- 查询机构
   QUERY_ORG                      string,
   -- 客户号
   CUST_NO                        string,
   -- 卡号
   CARD_NO                        string,
   -- 身份证号
   CRDT_NO                        string,
   -- 日志日期
   P9_DATA_DATE                   string
)
PARTITIONED BY (LOAD_DATE string)
STORED AS TEXTFILE;

-- ORC内部表，节约存储空间
DROP TABLE IF EXISTS INN_P2LOG_CCBS_RES;

CREATE TABLE IF NOT EXISTS INN_P2LOG_CCBS_RES(
   -- 查询日期
   QUE_DATE                       string,
   -- 查询时间
   QUE_TIME                       string,
   -- 柜员号
   TLR_ID                         string,
   -- 
   APP_NO                         string,
   -- 查询机构
   QUERY_ORG                      string,
   -- 客户号
   CUST_NO                        string,
   -- 卡号
   CARD_NO                        string,
   -- 身份证号
   CRDT_NO                        string
)
PARTITIONED BY (P9_DATA_DATE string)
STORED AS ORC;

