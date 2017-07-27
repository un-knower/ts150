use sor;

-- Hive贴源数据处理
-- 综合前置客户查询流水: P2LOG_CCBS_RES

-- 指定新数据日期分区位置
ALTER TABLE EXT_P2LOG_CCBS_RES DROP IF EXISTS PARTITION(LOAD_DATE='${log_date}');
ALTER TABLE EXT_P2LOG_CCBS_RES ADD PARTITION (LOAD_DATE='${log_date}') LOCATION 'hdfs://hacluster/bigdata/input/TS150/case_trace/P2LOG_CCBS_RES/${log_date}/';

-- 备份贴源数据到ORC内部表
INSERT OVERWRITE TABLE INN_P2LOG_CCBS_RES PARTITION(P9_DATA_DATE='${log_date}')
SELECT 
   -- 查询日期
   regexp_replace(QUE_DATE, '-', ''),
   -- 查询时间
   QUE_TIME,
   -- 柜员号
   TLR_ID,
   -- 
   APP_NO,
   -- 查询机构
   QUERY_ORG,
   -- 客户号
   CUST_NO,
   -- 卡号
   CARD_NO,
   -- 身份证号
   CRDT_NO
  FROM EXT_P2LOG_CCBS_RES
 WHERE LOAD_DATE='${log_date}';
