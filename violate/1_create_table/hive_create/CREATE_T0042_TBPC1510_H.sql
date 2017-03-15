use sor;

-- Hive建表脚本
-- 客户联系位置信息: T0042_TBPC1510_H

-- 外部表
DROP TABLE IF EXISTS EXT_T0042_TBPC1510_H;

CREATE EXTERNAL TABLE IF NOT EXISTS EXT_T0042_TBPC1510_H(
   -- #多实体标识
   MULTI_TENANCY_ID               string,
   -- 客户编号
   CST_ID                         string,
   -- #记录失效时间戳
   RCRD_EXPY_TMS                  string,
   -- 联系信息类型代码
   CTC_INF_TPCD                   string,
   -- #个人联系信息序号
   IDV_CTC_INF_SN                 string,
   -- 关系类型代码
   RETPCD                         string,
   -- 电话联系方式国际区号
   TELCTCMOD_ITNL_DSTCNO          string,
   -- 电话联系方式国内区号
   TELCTCMOD_DMST_DSTCNO          string,
   -- 电话联系方式号码
   TELCTCMOD_NO                   string,
   -- 电话联系方式分机号码
   TELCTCMOD_EXN_NO               string,
   -- 邮政编码
   ZIPECD                         string,
   -- 国家地区代码
   CTYRGON_CD                     string,
   -- 省自治区代码
   PROV_ATNMSRGON_CD              string,
   -- 城市代码
   URBN_CD                        string,
   -- 区县代码
   CNTYANDDSTC_CD                 string,
   -- 详细地址内容
   DTL_ADR_CNTNT                  string,
   -- 关系开始日期
   REL_STDT                       string,
   -- 关系结束日期
   REL_EDDT                       string,
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
DROP TABLE IF EXISTS INN_T0042_TBPC1510_H;

CREATE TABLE IF NOT EXISTS INN_T0042_TBPC1510_H(
   -- #多实体标识
   MULTI_TENANCY_ID               string,
   -- 客户编号
   CST_ID                         string,
   -- #记录失效时间戳
   RCRD_EXPY_TMS                  string,
   -- 联系信息类型代码
   CTC_INF_TPCD                   string,
   -- #个人联系信息序号
   IDV_CTC_INF_SN                 string,
   -- 关系类型代码
   RETPCD                         string,
   -- 电话联系方式国际区号
   TELCTCMOD_ITNL_DSTCNO          string,
   -- 电话联系方式国内区号
   TELCTCMOD_DMST_DSTCNO          string,
   -- 电话联系方式号码
   TELCTCMOD_NO                   string,
   -- 电话联系方式分机号码
   TELCTCMOD_EXN_NO               string,
   -- 邮政编码
   ZIPECD                         string,
   -- 国家地区代码
   CTYRGON_CD                     string,
   -- 省自治区代码
   PROV_ATNMSRGON_CD              string,
   -- 城市代码
   URBN_CD                        string,
   -- 区县代码
   CNTYANDDSTC_CD                 string,
   -- 详细地址内容
   DTL_ADR_CNTNT                  string,
   -- 关系开始日期
   REL_STDT                       string,
   -- 关系结束日期
   REL_EDDT                       string,
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
DROP TABLE IF EXISTS CT_T0042_TBPC1510_H_MID;

CREATE TABLE IF NOT EXISTS CT_T0042_TBPC1510_H_MID (
   -- 客户编号
   CST_ID                         string,
   -- 联系信息类型代码
   CTC_INF_TPCD                   string,
   -- 电话联系方式号码
   TELCTCMOD_NO                   string,
   -- 电话联系方式分机号码
   TELCTCMOD_EXN_NO               string,
   -- 详细地址内容
   DTL_ADR_CNTNT                  string,
   -- P9开始日期
   P9_START_DATE                  string,
   -- P9结束日期
   P9_END_DATE                    string
)
PARTITIONED BY (DATA_TYPE string)
STORED AS ORC;

-- 最终拉链表
DROP TABLE IF EXISTS CT_T0042_TBPC1510_H;

CREATE TABLE IF NOT EXISTS CT_T0042_TBPC1510_H (
   -- 客户编号
   CST_ID                         string,
   -- 联系信息类型代码
   CTC_INF_TPCD                   string,
   -- 电话联系方式号码
   TELCTCMOD_NO                   string,
   -- 电话联系方式分机号码
   TELCTCMOD_EXN_NO               string,
   -- 详细地址内容
   DTL_ADR_CNTNT                  string,
   -- P9开始日期
   P9_START_DATE                  string
)
PARTITIONED BY (P9_END_DATE string)
STORED AS ORC;
