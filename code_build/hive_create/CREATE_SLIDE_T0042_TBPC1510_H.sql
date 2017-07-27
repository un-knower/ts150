use sor;

-- Hive建表脚本
-- 客户联系位置信息: T0042_TBPC1510_H

-- 外部表
DROP TABLE IF EXISTS EXT_T0042_TBPC1510_H;

CREATE EXTERNAL TABLE IF NOT EXISTS EXT_T0042_TBPC1510_H(
    MULTI_TENANCY_ID          string comment '多实体标识',
    CST_ID                    string comment '客户编号',
    RCRD_EXPY_TMS             string comment '记录失效时间戳',
    CTC_INF_TPCD              string comment '联系信息类型代码',
    IDV_CTC_INF_SN            string comment '个人联系信息序号',
    RETPCD                    string comment '关系类型代码',
    TELCTCMOD_ITNL_DSTCNO     string comment '电话联系方式国际区号',
    TELCTCMOD_DMST_DSTCNO     string comment '电话联系方式国内区号',
    TELCTCMOD_NO              string comment '电话联系方式号码',
    TELCTCMOD_EXN_NO          string comment '电话联系方式分机号码',
    ZIPECD                    string comment '邮政编码',
    CTYRGON_CD                string comment '国家地区代码',
    PROV_ATNMSRGON_CD         string comment '省自治区代码',
    URBN_CD                   string comment '城市代码',
    CNTYANDDSTC_CD            string comment '区县代码',
    DTL_ADR_CNTNT             string comment '详细地址内容',
    REL_STDT                  string comment '关系开始日期',
    REL_EDDT                  string comment '关系结束日期',
    CRT_INSID                 string comment '创建机构编号',
    CRT_EMPID                 string comment '创建员工编号',
    LAST_UDT_INSID            string comment '最后更新机构编号',
    LAST_UDT_EMPID            string comment '最后更新员工编号',
    CUR_STM_CRT_TMS           string comment '当前系统创建时间戳',
    CUR_STM_UDT_TMS           string comment '当前系统更新时间戳',
    LCL_YRMO_DAY              string comment '本地年月日',
    LCL_HR_GRD_SCND           string comment '本地时分秒',
    CRT_STM_NO                string comment '创建系统号',
    UDT_STM_NO                string comment '更新系统号',
    SRCSYS_CRT_TMS            string comment '源系统创建时间戳',
    SRCSYS_UDT_TMS            string comment '源系统更新时间戳',
    P9_START_DATE             string comment 'P9开始日期',
    P9_START_BATCH            string comment '',
    P9_END_DATE               string comment 'P9结束日期',
    P9_END_BATCH              string comment '',
    P9_DEL_FLAG               string comment '',
    P9_JOB_NAME               string comment ''
)
PARTITIONED BY (LOAD_DATE string)
STORED AS TEXTFILE;

-- ORC内部表，节约存储空间
DROP TABLE IF EXISTS INN_T0042_TBPC1510_H;

CREATE TABLE IF NOT EXISTS INN_T0042_TBPC1510_H(
    MULTI_TENANCY_ID          string comment '多实体标识',
    CST_ID                    string comment '客户编号',
    RCRD_EXPY_TMS             string comment '记录失效时间戳',
    CTC_INF_TPCD              string comment '联系信息类型代码',
    IDV_CTC_INF_SN            string comment '个人联系信息序号',
    RETPCD                    string comment '关系类型代码',
    TELCTCMOD_ITNL_DSTCNO     string comment '电话联系方式国际区号',
    TELCTCMOD_DMST_DSTCNO     string comment '电话联系方式国内区号',
    TELCTCMOD_NO              string comment '电话联系方式号码',
    TELCTCMOD_EXN_NO          string comment '电话联系方式分机号码',
    ZIPECD                    string comment '邮政编码',
    CTYRGON_CD                string comment '国家地区代码',
    PROV_ATNMSRGON_CD         string comment '省自治区代码',
    URBN_CD                   string comment '城市代码',
    CNTYANDDSTC_CD            string comment '区县代码',
    DTL_ADR_CNTNT             string comment '详细地址内容',
    REL_STDT                  string comment '关系开始日期',
    REL_EDDT                  string comment '关系结束日期',
    CRT_INSID                 string comment '创建机构编号',
    CRT_EMPID                 string comment '创建员工编号',
    LAST_UDT_INSID            string comment '最后更新机构编号',
    LAST_UDT_EMPID            string comment '最后更新员工编号',
    CUR_STM_CRT_TMS           string comment '当前系统创建时间戳',
    CUR_STM_UDT_TMS           string comment '当前系统更新时间戳',
    LCL_YRMO_DAY              string comment '本地年月日',
    LCL_HR_GRD_SCND           string comment '本地时分秒',
    CRT_STM_NO                string comment '创建系统号',
    UDT_STM_NO                string comment '更新系统号',
    SRCSYS_CRT_TMS            string comment '源系统创建时间戳',
    SRCSYS_UDT_TMS            string comment '源系统更新时间戳',
    P9_START_DATE             string comment 'P9开始日期',
    P9_END_DATE               string comment 'P9结束日期'
)
PARTITIONED BY (LOAD_DATE string)
STORED AS ORC;

-- 拉链表中间数据
DROP TABLE IF EXISTS CT_T0042_TBPC1510_H_MID;

CREATE TABLE IF NOT EXISTS CT_T0042_TBPC1510_H_MID (
    CST_ID                    string comment '客户编号',
    CTC_INF_TPCD              string comment '联系信息类型代码',
    TELCTCMOD_DMST_DSTCNO     string comment '电话联系方式国内区号',
    TELCTCMOD_NO              string comment '电话联系方式号码',
    TELCTCMOD_EXN_NO          string comment '电话联系方式分机号码',
    ZIPECD                    string comment '邮政编码',
    CTYRGON_CD                string comment '国家地区代码',
    PROV_ATNMSRGON_CD         string comment '省自治区代码',
    DTL_ADR_CNTNT             string comment '详细地址内容',
    P9_START_DATE             string comment 'P9开始日期',
    P9_END_DATE               string comment 'P9结束日期'
)
PARTITIONED BY (DATA_TYPE string)
STORED AS ORC;

-- 最终拉链表
DROP TABLE IF EXISTS CT_T0042_TBPC1510_H;

CREATE TABLE IF NOT EXISTS CT_T0042_TBPC1510_H (
    CST_ID                    string comment '客户编号',
    CTC_INF_TPCD              string comment '联系信息类型代码',
    TELCTCMOD_DMST_DSTCNO     string comment '电话联系方式国内区号',
    TELCTCMOD_NO              string comment '电话联系方式号码',
    TELCTCMOD_EXN_NO          string comment '电话联系方式分机号码',
    ZIPECD                    string comment '邮政编码',
    CTYRGON_CD                string comment '国家地区代码',
    PROV_ATNMSRGON_CD         string comment '省自治区代码',
    DTL_ADR_CNTNT             string comment '详细地址内容',
    P9_START_DATE             string comment 'P9开始日期'
)
PARTITIONED BY (P9_END_DATE string)
STORED AS ORC;
