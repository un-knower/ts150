use sor;

-- Hive建表脚本
-- 建行机构关系: T0651_CCBINS_REL_H

-- 外部表
DROP TABLE IF EXISTS EXT_T0651_CCBINS_REL_H;

CREATE EXTERNAL TABLE IF NOT EXISTS EXT_T0651_CCBINS_REL_H(
   -- 主建行机构编号
   PRIM_CCBINS_ID                 string,
   -- 建行机构内部层级结构类型代码
   CCBINSINRHIERSTCTP_CD          string,
   -- 建行机构层级结构描述
   CCBINS_HIERSTC_DSC             string,
   -- 建行机构层级结构名称
   CCBINS_HIERSTC_NM              string,
   -- 建行机构层级结构状态类型代码
   CCBINSHIERSTCSTTP_CD           string,
   -- 从建行机构编号
   SCDY_CCBINS_ID                 string,
   -- 建行机构和建行机构关系类型代码
   CCBIANDCCBIRELTP_CD            string,
   -- 关系方向描述
   REL_DRC_DSC                    string,
   -- 建行机构和建行机构关系生命周期状态代码
   CCBIACCBI_REL_LCS_CD           string,
   -- 建行机构和建行机构关系生命周期状态日期
   CCBIACCBI_REL_LCS_DT           string,
   -- 建行机构和建行机构关系生命周期状态原因描述
   CCBIANDCCBIRLCSR_DSC           string,
   -- 建行机构和建行机构关系生命周期变更序号
   CCBIACCBI_REL_LCMD_SN          string,
   -- 机构管理等级类型代码
   INSTMGTGRD_TPCD                string,
   -- 机构管理等级类型取值代码
   INSTMGTGRD_TP_VAL_CD           string,
   -- 机构管理等级描述
   INSTMGTGRD_DSC                 string,
   -- 关系场景描述
   REL_SCN_DSC                    string,
   -- #最后更新日期时间
   LAST_UDT_DT_TM                 string,
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
DROP TABLE IF EXISTS INN_T0651_CCBINS_REL_H;

CREATE TABLE IF NOT EXISTS INN_T0651_CCBINS_REL_H(
   -- 主建行机构编号
   PRIM_CCBINS_ID                 string,
   -- 建行机构内部层级结构类型代码
   CCBINSINRHIERSTCTP_CD          string,
   -- 建行机构层级结构描述
   CCBINS_HIERSTC_DSC             string,
   -- 建行机构层级结构名称
   CCBINS_HIERSTC_NM              string,
   -- 建行机构层级结构状态类型代码
   CCBINSHIERSTCSTTP_CD           string,
   -- 从建行机构编号
   SCDY_CCBINS_ID                 string,
   -- 建行机构和建行机构关系类型代码
   CCBIANDCCBIRELTP_CD            string,
   -- 关系方向描述
   REL_DRC_DSC                    string,
   -- 建行机构和建行机构关系生命周期状态代码
   CCBIACCBI_REL_LCS_CD           string,
   -- 建行机构和建行机构关系生命周期状态日期
   CCBIACCBI_REL_LCS_DT           string,
   -- 建行机构和建行机构关系生命周期状态原因描述
   CCBIANDCCBIRLCSR_DSC           string,
   -- 建行机构和建行机构关系生命周期变更序号
   CCBIACCBI_REL_LCMD_SN          string,
   -- 机构管理等级类型代码
   INSTMGTGRD_TPCD                string,
   -- 机构管理等级类型取值代码
   INSTMGTGRD_TP_VAL_CD           string,
   -- 机构管理等级描述
   INSTMGTGRD_DSC                 string,
   -- 关系场景描述
   REL_SCN_DSC                    string,
   -- #最后更新日期时间
   LAST_UDT_DT_TM                 string,
   -- P9开始日期
   P9_START_DATE                  string,
   -- P9结束日期
   P9_END_DATE                    string
)
PARTITIONED BY (LOAD_DATE string)
STORED AS ORC;


-- 拉链表中间数据
DROP TABLE IF EXISTS CT_T0651_CCBINS_REL_H_MID;

CREATE TABLE IF NOT EXISTS CT_T0651_CCBINS_REL_H_MID (
   -- 主建行机构编号
   PRIM_CCBINS_ID                 string,
   -- 建行机构内部层级结构类型代码
   CCBINSINRHIERSTCTP_CD          string,
   -- 建行机构层级结构名称
   CCBINS_HIERSTC_NM              string,
   -- 从建行机构编号
   SCDY_CCBINS_ID                 string,
   -- 机构管理等级描述
   INSTMGTGRD_DSC                 string,
   -- 关系场景描述
   REL_SCN_DSC                    string,
   -- #最后更新日期时间
   LAST_UDT_DT_TM                 string,
   -- P9开始日期
   P9_START_DATE                  string,
   -- P9结束日期
   P9_END_DATE                    string
)
PARTITIONED BY (DATA_TYPE string)
STORED AS ORC;

-- 最终拉链表
DROP TABLE IF EXISTS CT_T0651_CCBINS_REL_H;

CREATE TABLE IF NOT EXISTS CT_T0651_CCBINS_REL_H (
   -- 主建行机构编号
   PRIM_CCBINS_ID                 string,
   -- 建行机构内部层级结构类型代码
   CCBINSINRHIERSTCTP_CD          string,
   -- 建行机构层级结构名称
   CCBINS_HIERSTC_NM              string,
   -- 从建行机构编号
   SCDY_CCBINS_ID                 string,
   -- 机构管理等级描述
   INSTMGTGRD_DSC                 string,
   -- 关系场景描述
   REL_SCN_DSC                    string,
   -- #最后更新日期时间
   LAST_UDT_DT_TM                 string,
   -- P9开始日期
   P9_START_DATE                  string
)
PARTITIONED BY (P9_END_DATE string)
STORED AS ORC;
