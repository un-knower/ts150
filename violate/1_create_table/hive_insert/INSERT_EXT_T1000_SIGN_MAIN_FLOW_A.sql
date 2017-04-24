use sor;

-- Hive贴源数据处理
-- 签约主流水表: T1000_SIGN_MAIN_FLOW_A

-- 指定新数据日期分区位置
ALTER TABLE EXT_T1000_SIGN_MAIN_FLOW_A DROP IF EXISTS PARTITION(LOAD_DATE='${log_date}');
ALTER TABLE EXT_T1000_SIGN_MAIN_FLOW_A ADD PARTITION (LOAD_DATE='${log_date}') LOCATION 'hdfs://hacluster/bigdata/input/TS150/case_trace/T1000_SIGN_MAIN_FLOW_A/${log_date}/';

-- 备份贴源数据到ORC内部表
INSERT OVERWRITE TABLE INN_T1000_SIGN_MAIN_FLOW_A PARTITION(P9_DATA_DATE='${log_date}')
SELECT 
   -- 交易流水号
   FLOW_NO,
   -- 流水号
   CHANL_FLOW_NO,
   -- ecif流水号
   ECIF_FLOW_NO,
   -- ECIF客户编号
   ECIF_CUST_NO,
   -- CCBS客户编号
   CUST_NO,
   -- 渠道客户号
   CHANL_CUST_NO,
   -- 证件类型
   CERT_TYP,
   -- 证件号码
   CERT_ID,
   -- 户名
   CUST_NM,
   -- 交易码
   CHANL_TRAD_NO,
   -- 渠道编号
   CHANL_NO,
   -- 签约途径
   SIGN_APPR,
   -- 签约机构
   SIGN_BRAN,
   -- 开通签约服务柜员
   SIGN_TLR,
   -- 签约日期
   SIGN_DATE,
   -- 交易时间
   SIGN_TIME,
   -- #终端信息
   TERM_INF,
   -- 成功标志
   TRAD_STS,
   -- 响应码
   RETURN_COD,
   -- 响应信息
   RETURN_MSG,
   -- 说明1
   REMARK1,
   -- 说明2
   REMARK2,
   -- 备注3
   REMARK3
  FROM EXT_T1000_SIGN_MAIN_FLOW_A
 WHERE LOAD_DATE='${log_date}';
