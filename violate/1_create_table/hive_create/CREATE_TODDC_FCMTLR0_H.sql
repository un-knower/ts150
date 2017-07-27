use sor;

-- Hive建表脚本
-- 柜员文件: TODDC_FCMTLR0_H

-- 外部表
DROP TABLE IF EXISTS EXT_TODDC_FCMTLR0_H;

CREATE EXTERNAL TABLE IF NOT EXISTS EXT_TODDC_FCMTLR0_H(
   -- 操作员号
   CM_OPR_NO                      string,
   -- 时间戳
   CMTLR_DB_TIMESTAMP             string,
   -- 操作员姓名
   CM_OPR_NAME                    string,
   -- 身份证号码
   CM_ID_NO                       string,
   -- 柜员备注
   CM_TLR_RMRK                    string,
   -- 作业等级
   CM_OPN_RANG                    string,
   -- 密码
   CM_OPR_PSWD                    string,
   -- 密码最后更正日期
   CM_PSWD_FINAL_UPDT_DT          string,
   -- 通汇免授权金额
   CM_CORS_FAUTH_AMT              string,
   -- 最近作业日
   CM_LTST_OPN_DT                 string,
   -- 签到注记
   CM_SGNI_RMRK                   string,
   -- 本日签到时间
   CM_CRNT_DAY_SGNI_TM            string,
   -- 本日签退时间
   CM_CRNT_DAY_SGNO_TM            string,
   -- 资料最后更正日期
   CM_DATA_LST_UPDT_DT            string,
   -- 柜员状态
   CM_TLR_STS                     string,
   -- 操作卡号
   CM_OPR_CRD_NO                  string,
   -- 可执行交易组
   CM_EXCS_TX_SECN                string,
   -- 空白
   FILLER1                        string,
   -- 柜员建档日期
   CM_TLR_FLST_DT                 string,
   -- 柜员签到错误次数
   CM_TLR_SG1_ERR_TM              string,
   -- 密钥代码
   CM_TSTKY_COD                   string,
   -- 押码密钥
   CM_TEST_KEY                    string,
   -- 柜员当日帐务流水序号
   CM_CUR_TX_SRL                  string,
   -- 柜员次日帐务流水序号
   CM_NEXT_TX_SRL                 string,
   -- 柜员当日非帐务流水序号
   CM_CUR_NON_TX_SRL              string,
   -- 柜员次日非帐务流水序号
   CM_NEXT_NON_TX_SRL             string,
   -- 预留流水笔数
   CM_REV_TX_NUMBER               string,
   -- 操作员卡号2
   CM_OPR_CRD_NO2                 string,
   -- 前置柜员签到错误次数
   CM_FRONT_TLR_SG1_ERR_TM        string,
   -- 金库柜员标志
   CM_TLR_CASH_FLAG               string,
   -- 现金管理员标志
   CM_TLR_RNG_CSHR_FLG            string,
   -- 原始等级
   CM_OPN_RANG_INIT               string,
   -- 转出柜员号
   CM_OUT_TLR_OPR                 string,
   -- 转出授权标志
   CM_OUT_AUTH_FLG                string,
   -- 转出生效日期
   CM_OUT_AVL_DATE                string,
   -- 转出失效日期
   CM_OUT_INAVL_DATE              string,
   -- 转入柜员号
   CM_IN_TLR_OPR                  string,
   -- 转入授权标志
   CM_IN_AUTH_FLG                 string,
   -- 转入生效日期
   CM_IN_AVL_DATE                 string,
   -- 转入失效日期
   CM_IN_UNAVL_DATE               string,
   -- 所属机构号
   CM_OPUN_CODE                   string,
   -- 柜员流水子码
   CM_SEQ_NO                      string,
   -- 尾箱号
   CM_CSHBX_NO                    string,
   -- 证券业务范围
   CM_CCBSS_TELLER_GRAD           string,
   -- 用户类型
   CM_USER_TYPE                   string,
   -- #FILLER
   FILLER                         string,
   -- 按机构拆分字段
   P9_SPLIT_BRANCH_CD             string,
   -- P9开始日期
   P9_START_DATE                  string,
   -- P9开始批次号
   P9_START_BATCH                 string,
   -- P9结束日期
   P9_END_DATE                    string,
   -- P9结束批次号
   P9_END_BATCH                   string,
   -- P9删除标志
   P9_DEL_FLAG                    string,
   -- P9作业名
   P9_JOB_NAME                    string
)
PARTITIONED BY (LOAD_DATE string)
STORED AS TEXTFILE;

-- ORC内部表，节约存储空间
DROP TABLE IF EXISTS INN_TODDC_FCMTLR0_H;

CREATE TABLE IF NOT EXISTS INN_TODDC_FCMTLR0_H(
   -- 操作员号
   CM_OPR_NO                      string,
   -- 时间戳
   CMTLR_DB_TIMESTAMP             string,
   -- 操作员姓名
   CM_OPR_NAME                    string,
   -- 身份证号码
   CM_ID_NO                       string,
   -- 柜员备注
   CM_TLR_RMRK                    string,
   -- 作业等级
   CM_OPN_RANG                    string,
   -- 密码
   CM_OPR_PSWD                    string,
   -- 密码最后更正日期
   CM_PSWD_FINAL_UPDT_DT          string,
   -- 通汇免授权金额
   CM_CORS_FAUTH_AMT              string,
   -- 最近作业日
   CM_LTST_OPN_DT                 string,
   -- 签到注记
   CM_SGNI_RMRK                   string,
   -- 本日签到时间
   CM_CRNT_DAY_SGNI_TM            string,
   -- 本日签退时间
   CM_CRNT_DAY_SGNO_TM            string,
   -- 资料最后更正日期
   CM_DATA_LST_UPDT_DT            string,
   -- 柜员状态
   CM_TLR_STS                     string,
   -- 操作卡号
   CM_OPR_CRD_NO                  string,
   -- 可执行交易组
   CM_EXCS_TX_SECN                string,
   -- 空白
   FILLER1                        string,
   -- 柜员建档日期
   CM_TLR_FLST_DT                 string,
   -- 柜员签到错误次数
   CM_TLR_SG1_ERR_TM              string,
   -- 密钥代码
   CM_TSTKY_COD                   string,
   -- 押码密钥
   CM_TEST_KEY                    string,
   -- 柜员当日帐务流水序号
   CM_CUR_TX_SRL                  string,
   -- 柜员次日帐务流水序号
   CM_NEXT_TX_SRL                 string,
   -- 柜员当日非帐务流水序号
   CM_CUR_NON_TX_SRL              string,
   -- 柜员次日非帐务流水序号
   CM_NEXT_NON_TX_SRL             string,
   -- 预留流水笔数
   CM_REV_TX_NUMBER               string,
   -- 操作员卡号2
   CM_OPR_CRD_NO2                 string,
   -- 前置柜员签到错误次数
   CM_FRONT_TLR_SG1_ERR_TM        string,
   -- 金库柜员标志
   CM_TLR_CASH_FLAG               string,
   -- 现金管理员标志
   CM_TLR_RNG_CSHR_FLG            string,
   -- 原始等级
   CM_OPN_RANG_INIT               string,
   -- 转出柜员号
   CM_OUT_TLR_OPR                 string,
   -- 转出授权标志
   CM_OUT_AUTH_FLG                string,
   -- 转出生效日期
   CM_OUT_AVL_DATE                string,
   -- 转出失效日期
   CM_OUT_INAVL_DATE              string,
   -- 转入柜员号
   CM_IN_TLR_OPR                  string,
   -- 转入授权标志
   CM_IN_AUTH_FLG                 string,
   -- 转入生效日期
   CM_IN_AVL_DATE                 string,
   -- 转入失效日期
   CM_IN_UNAVL_DATE               string,
   -- 所属机构号
   CM_OPUN_CODE                   string,
   -- 柜员流水子码
   CM_SEQ_NO                      string,
   -- 尾箱号
   CM_CSHBX_NO                    string,
   -- 证券业务范围
   CM_CCBSS_TELLER_GRAD           string,
   -- 用户类型
   CM_USER_TYPE                   string,
   -- #FILLER
   FILLER                         string,
   -- P9开始日期
   P9_START_DATE                  string,
   -- P9结束日期
   P9_END_DATE                    string
)
PARTITIONED BY (LOAD_DATE string)
STORED AS ORC;


-- 拉链表中间数据
DROP TABLE IF EXISTS CT_TODDC_FCMTLR0_H_MID;

CREATE TABLE IF NOT EXISTS CT_TODDC_FCMTLR0_H_MID (
   -- 操作员号
   CM_OPR_NO                      string,
   -- 操作员姓名
   CM_OPR_NAME                    string,
   -- 身份证号码
   CM_ID_NO                       string,
   -- 柜员备注
   CM_TLR_RMRK                    string,
   -- 柜员状态
   CM_TLR_STS                     string,
   -- 所属机构号
   CM_OPUN_CODE                   string,
   -- P9开始日期
   P9_START_DATE                  string,
   -- P9结束日期
   P9_END_DATE                    string
)
PARTITIONED BY (DATA_TYPE string)
STORED AS ORC;

-- 最终拉链表
DROP TABLE IF EXISTS CT_TODDC_FCMTLR0_H;

CREATE TABLE IF NOT EXISTS CT_TODDC_FCMTLR0_H (
   -- 操作员号
   CM_OPR_NO                      string,
   -- 操作员姓名
   CM_OPR_NAME                    string,
   -- 身份证号码
   CM_ID_NO                       string,
   -- 柜员备注
   CM_TLR_RMRK                    string,
   -- 柜员状态
   CM_TLR_STS                     string,
   -- 所属机构号
   CM_OPUN_CODE                   string,
   -- P9开始日期
   P9_START_DATE                  string
)
PARTITIONED BY (P9_END_DATE string)
STORED AS ORC;
