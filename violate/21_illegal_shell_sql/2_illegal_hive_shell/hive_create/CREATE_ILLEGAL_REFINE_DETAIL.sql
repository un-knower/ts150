use sor;

-- Hive建表脚本
-- ATM、非账务流水、签约等流水信息汇总: 
--ILLEGAL_REFINE_DETAIL_FCM : 提取重要非账务性流水表的日期、柜员机构、客户身份证号
--ILLEGAL_REFINE_DETAIL_ATM :提取ATM日期、柜员机构、客户身份证号
--ILLEGAL_REFINE_DETAIL_QY: 提取签约信息的日期、柜员机构、客户身份证号
--ILLEGAL_REFINE_DETAIL_GM:提取柜面信息的日期、柜员机构、客户身份证号
--ILLEGAL_REFINE_DETAIL:汇总所有的日期、柜员机构、客户身份证号

--非账务
DROP TABLE IF EXISTS ILLEGAL_REFINE_DETAIL_FCM_TMP;
CREATE TABLE IF NOT EXISTS ILLEGAL_REFINE_DETAIL_FCM_TMP(
  TX_DT STRING,
  OPER_CODE STRING,
  CM_ACCT_NO STRING,
  APLY_DSCRP STRING
)
PARTITIONED BY (P9_DATA_DATE STRING)
STORED AS ORC;

--非账务+机构
DROP TABLE IF EXISTS ILLEGAL_REFINE_DETAIL_FCM_ORG;
CREATE TABLE IF NOT EXISTS ILLEGAL_REFINE_DETAIL_FCM_ORG(
  TX_DT STRING,
  OPER_CODE STRING,
  CM_ACCT_NO STRING,
  APLY_DSCRP STRING,
  BRANCH_ID STRING
)
PARTITIONED BY (P9_DATA_DATE STRING)
STORED AS ORC;


--非账务+身份证、卡
DROP TABLE IF EXISTS ILLEGAL_REFINE_DETAIL_FCM;
CREATE TABLE IF NOT EXISTS ILLEGAL_REFINE_DETAIL_FCM(
  TX_DT STRING,
  OPER_CODE STRING,
  BRANCH_ID STRING,
  CARD_NO STRING,
  PASS_NUM STRING,
  APLY_DSCRP STRING
)
PARTITIONED BY (P9_DATA_DATE STRING)
STORED AS ORC;









--ATM
DROP TABLE IF EXISTS ILLEGAL_REFINE_DETAIL_ATM;

CREATE EXTERNAL TABLE IF NOT EXISTS ILLEGAL_REFINE_DETAIL_ATM(
  TX_DT STRING,
  BRANCH_ID STRING,
  PASS_NUM STRING
)
PARTITIONED BY (P9_DATA_DATE string)
STORED AS ORC;


--提取签约主流水表
DROP TABLE IF EXISTS ILLEGAL_REFINE_DETAIL_QY;

CREATE EXTERNAL TABLE IF NOT EXISTS ILLEGAL_REFINE_DETAIL_QY(
  TX_DT STRING,
  BRANCH_ID STRING,
  PASS_NUM STRING
)
PARTITIONED BY (P9_DATA_DATE string)
STORED AS ORC;


--提取柜面交易流水表
DROP TABLE IF EXISTS ILLEGAL_REFINE_DETAIL_GM;

CREATE EXTERNAL TABLE IF NOT EXISTS ILLEGAL_REFINE_DETAIL_GM(
 TX_DT STRING,
  BRANCH_ID STRING,
  PASS_NUM STRING
)
PARTITIONED BY (P9_DATA_DATE string)
STORED AS ORC;


--汇总所有的交易流水信息
DROP TABLE IF EXISTS ILLEGAL_REFINE_DETAIL;

CREATE EXTERNAL TABLE IF NOT EXISTS ILLEGAL_REFINE_DETAIL(
  TX_DT STRING,
  BRANCH_ID STRING,
  PASS_NUM STRING
)
PARTITIONED BY (P9_DATA_DATE string)
STORED AS ORC;