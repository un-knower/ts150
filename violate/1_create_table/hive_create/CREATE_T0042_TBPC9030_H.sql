use sor;

-- Hive建表脚本
-- 内外客户号对照信息: T0042_TBPC9030_H

-- 外部表
DROP TABLE IF EXISTS EXT_T0042_TBPC9030_H;

CREATE EXTERNAL TABLE IF NOT EXISTS EXT_T0042_TBPC9030_H(
   -- #多实体标识
   MULTI_TENANCY_ID               string,
   -- #源信息系统代码
   SRC_INF_STM_CD                 string,
   -- #源系统客户编号
   SRCSYS_CST_ID                  string,
   -- #记录失效时间戳
   RCRD_EXPY_TMS                  string,
   -- 客户编号
   CST_ID                         string,
   -- 备用客户编号
   RSRV_CST_ID                    string,
   -- 创建机构编号
   CRT_INSID                      string,
   -- 创建员工编号
   CRT_EMPID                      string,
   -- 最后更新机构编号
   LAST_UDT_INSID                 string,
   -- 最后更新员工编号
   LAST_UDT_EMPID                 string,
   -- #当前系统创建时间戳
   CUR_STM_CRT_TMS                string,
   -- #当前系统更新时间戳
   CUR_STM_UDT_TMS                string,
   -- #本地年月日
   LCL_YRMO_DAY                   string,
   -- #本地时分秒
   LCL_HR_GRD_SCND                string,
   -- #创建系统号
   CRT_STM_NO                     string,
   -- #更新系统号
   UDT_STM_NO                     string,
   -- #源系统创建时间戳
   SRCSYS_CRT_TMS                 string,
   -- #源系统更新时间戳
   SRCSYS_UDT_TMS                 string,
   -- P9开始日期
   P9_START_DATE                  string,
   -- P9_START_BATCH
   P9_START_BATCH                 string,
   -- P9结束日期
   P9_END_DATE                    string,
   -- P9_END_BATCH
   P9_END_BATCH                   string,
   -- P9_DEL_FLAG
   P9_DEL_FLAG                    string,
   -- P9_JOB_NAME
   P9_JOB_NAME                    string
)
PARTITIONED BY (LOAD_DATE string)
STORED AS TEXTFILE;

-- ORC内部表，节约存储空间
DROP TABLE IF EXISTS INN_T0042_TBPC9030_H;

CREATE TABLE IF NOT EXISTS INN_T0042_TBPC9030_H(
   -- #多实体标识
   MULTI_TENANCY_ID               string,
   -- #源信息系统代码
   SRC_INF_STM_CD                 string,
   -- #源系统客户编号
   SRCSYS_CST_ID                  string,
   -- #记录失效时间戳
   RCRD_EXPY_TMS                  string,
   -- 客户编号
   CST_ID                         string,
   -- 备用客户编号
   RSRV_CST_ID                    string,
   -- 创建机构编号
   CRT_INSID                      string,
   -- 创建员工编号
   CRT_EMPID                      string,
   -- 最后更新机构编号
   LAST_UDT_INSID                 string,
   -- 最后更新员工编号
   LAST_UDT_EMPID                 string,
   -- #当前系统创建时间戳
   CUR_STM_CRT_TMS                string,
   -- #当前系统更新时间戳
   CUR_STM_UDT_TMS                string,
   -- #本地年月日
   LCL_YRMO_DAY                   string,
   -- #本地时分秒
   LCL_HR_GRD_SCND                string,
   -- #创建系统号
   CRT_STM_NO                     string,
   -- #更新系统号
   UDT_STM_NO                     string,
   -- #源系统创建时间戳
   SRCSYS_CRT_TMS                 string,
   -- #源系统更新时间戳
   SRCSYS_UDT_TMS                 string,
   -- P9开始日期
   P9_START_DATE                  string,
   -- P9结束日期
   P9_END_DATE                    string
)
PARTITIONED BY (LOAD_DATE string)
STORED AS ORC;


-- 拉链表中间数据
DROP TABLE IF EXISTS CT_T0042_TBPC9030_H_MID;

CREATE TABLE IF NOT EXISTS CT_T0042_TBPC9030_H_MID (
   -- #源信息系统代码
   SRC_INF_STM_CD                 string,
   -- #源系统客户编号
   SRCSYS_CST_ID                  string,
   -- 客户编号
   CST_ID                         string,
   -- P9开始日期
   P9_START_DATE                  string,
   -- P9结束日期
   P9_END_DATE                    string
)
PARTITIONED BY (DATA_TYPE string)
STORED AS ORC;

-- 最终拉链表
DROP TABLE IF EXISTS CT_T0042_TBPC9030_H;

CREATE TABLE IF NOT EXISTS CT_T0042_TBPC9030_H (
   -- #源信息系统代码
   SRC_INF_STM_CD                 string,
   -- #源系统客户编号
   SRCSYS_CST_ID                  string,
   -- 客户编号
   CST_ID                         string,
   -- P9开始日期
   P9_START_DATE                  string
)
PARTITIONED BY (P9_END_DATE string)
STORED AS ORC;
