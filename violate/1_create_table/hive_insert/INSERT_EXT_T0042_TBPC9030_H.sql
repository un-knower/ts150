use sor;

# Hive贴源数据处理
# 内外客户号对照信息: T0042_TBPC9030_H

-- 指定新数据日期分区位置
ALTER TABLE EXT_T0042_TBPC9030_H DROP IF EXISTS PARTITION(LOAD_DATE='${log_date}');
ALTER TABLE EXT_T0042_TBPC9030_H ADD PARTITION (LOAD_DATE='${log_date}') LOCATION 'hdfs://hacluster/bigdata/input/case_trace/T0042_TBPC9030_H/${log_date}/';

-- 备份贴源数据到ORC内部表
INSERT OVERWRITE TABLE INN_T0042_TBPC9030_H PARTITION(LOAD_DATE='${log_date}')
SELECT 
   -- #多实体标识
   MULTI_TENANCY_ID,
   -- #源信息系统代码
   SRC_INF_STM_CD,
   -- #源系统客户编号
   SRCSYS_CST_ID,
   -- #记录失效时间戳
   RCRD_EXPY_TMS,
   -- 客户编号
   CST_ID,
   -- 备用客户编号
   RSRV_CST_ID,
   -- 创建机构编号
   CRT_INSID,
   -- 创建员工编号
   CRT_EMPID,
   -- 最后更新机构编号
   LAST_UDT_INSID,
   -- 最后更新员工编号
   LAST_UDT_EMPID,
   -- #当前系统创建时间戳
   CUR_STM_CRT_TMS,
   -- #当前系统更新时间戳
   CUR_STM_UDT_TMS,
   -- #本地年月日
   LCL_YRMO_DAY,
   -- #本地时分秒
   LCL_HR_GRD_SCND,
   -- #创建系统号
   CRT_STM_NO,
   -- #更新系统号
   UDT_STM_NO,
   -- #源系统创建时间戳
   SRCSYS_CRT_TMS,
   -- #源系统更新时间戳
   SRCSYS_UDT_TMS,
   -- P9开始日期
   regexp_replace(P9_START_DATE, '-', ''),
   -- P9结束日期
   regexp_replace(P9_END_DATE, '-', '')
  FROM EXT_T0042_TBPC9030_H
 WHERE LOAD_DATE='${log_date}';
