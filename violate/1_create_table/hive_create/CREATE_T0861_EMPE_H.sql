use sor;

-- Hive建表脚本
-- 员工: T0861_EMPE_H

-- 外部表
DROP TABLE IF EXISTS EXT_T0861_EMPE_H;

CREATE EXTERNAL TABLE IF NOT EXISTS EXT_T0861_EMPE_H(
   -- 建行员工编号
   CCB_EMPID                      string,
   -- 性别代码
   GND_CD                         string,
   -- 用户姓名
   USR_NM                         string,
   -- 人力资源员工编号
   HMNRSC_EMPID                   string,
   -- 员工识别登录名
   EMPE_ID_LAND_NM                string,
   -- 证件类型代码
   CRDT_TPCD                      string,
   -- 证件号码
   CRDT_NO                        string,
   -- 用户类型代码
   USR_TPCD                       string,
   -- 用户状态代码
   USR_STCD                       string,
   -- 电话联系方式办公号码
   TELCTCMOD_WRK_NO               string,
   -- 电话联系方式手机号码
   TELCTCMOD_MBLPH_NO             string,
   -- 电话联系方式传真号码
   TELCTCMOD_FAX_NO               string,
   -- 电话联系方式备用号码
   TELCTCMOD_RSRV_NO              string,
   -- 电子邮件地址内容
   EMAIL_ADR_CNTNT                string,
   -- 个人名称姓氏
   IDV_NM_SURNM                   string,
   -- 个人名称名
   IDV_NM_NM                      string,
   -- 邮政编码
   ZIPECD                         string,
   -- 详细地址
   DTL_ADR                        string,
   -- 所属机构编号
   BLNG_INSID                     string,
   -- 工作单位名称
   WRK_UNIT_NM                    string,
   -- 外部用户职务
   EXT_USR_POST                   string,
   -- 学历代码
   EDDGR_CD                       string,
   -- 学位代码
   DGR_CD                         string,
   -- 毕业学校代码
   GRDT_SCH_CD                    string,
   -- 学校性质代码
   SCH_CHAR_CD                    string,
   -- 所学专业代码
   MJR_CD                         string,
   -- 学习形式代码
   STDY_FORM_CD                   string,
   -- 用户姓名拼音全称
   USR_NM_CPA_FULLNM              string,
   -- 员工岗位锁定状态代码
   EMPE_PST_LOCK_STCD             string,
   -- 生效日期时间
   EFDT_TM                        string,
   -- 创建系统管理操作员编号
   CRT_STM_MGT_OPR_ID             string,
   -- #最后更新日期时间
   LAST_UDT_DT_TM                 string,
   -- 其他专业名称
   OTHR_PROF_NM                   string,
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
   P9_JOB_NAME                    string,
   -- 员工照片文件索引号
   EMPE_PIC_FILE_INDX_NO          string,
   -- 出生日期
   BRTH_DT                        string,
   -- 紧急联系方式内容
   EMGR_CTC_MOD_CNTNT             string,
   -- 毕业日期
   GRDT_DT                        string
)
PARTITIONED BY (LOAD_DATE string)
STORED AS TEXTFILE;

-- ORC内部表，节约存储空间
DROP TABLE IF EXISTS INN_T0861_EMPE_H;

CREATE TABLE IF NOT EXISTS INN_T0861_EMPE_H(
   -- 建行员工编号
   CCB_EMPID                      string,
   -- 性别代码
   GND_CD                         string,
   -- 用户姓名
   USR_NM                         string,
   -- 人力资源员工编号
   HMNRSC_EMPID                   string,
   -- 员工识别登录名
   EMPE_ID_LAND_NM                string,
   -- 证件类型代码
   CRDT_TPCD                      string,
   -- 证件号码
   CRDT_NO                        string,
   -- 用户类型代码
   USR_TPCD                       string,
   -- 用户状态代码
   USR_STCD                       string,
   -- 电话联系方式办公号码
   TELCTCMOD_WRK_NO               string,
   -- 电话联系方式手机号码
   TELCTCMOD_MBLPH_NO             string,
   -- 电话联系方式传真号码
   TELCTCMOD_FAX_NO               string,
   -- 电话联系方式备用号码
   TELCTCMOD_RSRV_NO              string,
   -- 电子邮件地址内容
   EMAIL_ADR_CNTNT                string,
   -- 个人名称姓氏
   IDV_NM_SURNM                   string,
   -- 个人名称名
   IDV_NM_NM                      string,
   -- 邮政编码
   ZIPECD                         string,
   -- 详细地址
   DTL_ADR                        string,
   -- 所属机构编号
   BLNG_INSID                     string,
   -- 工作单位名称
   WRK_UNIT_NM                    string,
   -- 外部用户职务
   EXT_USR_POST                   string,
   -- 学历代码
   EDDGR_CD                       string,
   -- 学位代码
   DGR_CD                         string,
   -- 毕业学校代码
   GRDT_SCH_CD                    string,
   -- 学校性质代码
   SCH_CHAR_CD                    string,
   -- 所学专业代码
   MJR_CD                         string,
   -- 学习形式代码
   STDY_FORM_CD                   string,
   -- 用户姓名拼音全称
   USR_NM_CPA_FULLNM              string,
   -- 员工岗位锁定状态代码
   EMPE_PST_LOCK_STCD             string,
   -- 生效日期时间
   EFDT_TM                        string,
   -- 创建系统管理操作员编号
   CRT_STM_MGT_OPR_ID             string,
   -- #最后更新日期时间
   LAST_UDT_DT_TM                 string,
   -- 其他专业名称
   OTHR_PROF_NM                   string,
   -- P9开始日期
   P9_START_DATE                  string,
   -- P9结束日期
   P9_END_DATE                    string,
   -- 员工照片文件索引号
   EMPE_PIC_FILE_INDX_NO          string,
   -- 出生日期
   BRTH_DT                        string,
   -- 紧急联系方式内容
   EMGR_CTC_MOD_CNTNT             string,
   -- 毕业日期
   GRDT_DT                        string
)
PARTITIONED BY (LOAD_DATE string)
STORED AS ORC;


-- 拉链表中间数据
DROP TABLE IF EXISTS CT_T0861_EMPE_H_MID;

CREATE TABLE IF NOT EXISTS CT_T0861_EMPE_H_MID (
   -- 建行员工编号
   CCB_EMPID                      string,
   -- 性别代码
   GND_CD                         string,
   -- 用户姓名
   USR_NM                         string,
   -- 人力资源员工编号
   HMNRSC_EMPID                   string,
   -- 员工识别登录名
   EMPE_ID_LAND_NM                string,
   -- 证件类型代码
   CRDT_TPCD                      string,
   -- 证件号码
   CRDT_NO                        string,
   -- 用户状态代码
   USR_STCD                       string,
   -- 电话联系方式办公号码
   TELCTCMOD_WRK_NO               string,
   -- 电话联系方式手机号码
   TELCTCMOD_MBLPH_NO             string,
   -- 个人名称姓氏
   IDV_NM_SURNM                   string,
   -- 个人名称名
   IDV_NM_NM                      string,
   -- 详细地址
   DTL_ADR                        string,
   -- 所属机构编号
   BLNG_INSID                     string,
   -- 生效日期时间
   EFDT_TM                        string,
   -- P9开始日期
   P9_START_DATE                  string,
   -- P9结束日期
   P9_END_DATE                    string
)
PARTITIONED BY (DATA_TYPE string)
STORED AS ORC;

-- 最终拉链表
DROP TABLE IF EXISTS CT_T0861_EMPE_H;

CREATE TABLE IF NOT EXISTS CT_T0861_EMPE_H (
   -- 建行员工编号
   CCB_EMPID                      string,
   -- 性别代码
   GND_CD                         string,
   -- 用户姓名
   USR_NM                         string,
   -- 人力资源员工编号
   HMNRSC_EMPID                   string,
   -- 员工识别登录名
   EMPE_ID_LAND_NM                string,
   -- 证件类型代码
   CRDT_TPCD                      string,
   -- 证件号码
   CRDT_NO                        string,
   -- 用户状态代码
   USR_STCD                       string,
   -- 电话联系方式办公号码
   TELCTCMOD_WRK_NO               string,
   -- 电话联系方式手机号码
   TELCTCMOD_MBLPH_NO             string,
   -- 个人名称姓氏
   IDV_NM_SURNM                   string,
   -- 个人名称名
   IDV_NM_NM                      string,
   -- 详细地址
   DTL_ADR                        string,
   -- 所属机构编号
   BLNG_INSID                     string,
   -- 生效日期时间
   EFDT_TM                        string,
   -- P9开始日期
   P9_START_DATE                  string
)
PARTITIONED BY (P9_END_DATE string)
STORED AS ORC;
