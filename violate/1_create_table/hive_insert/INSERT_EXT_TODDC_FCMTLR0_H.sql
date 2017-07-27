use sor;

-- Hive贴源数据处理
-- 柜员文件: TODDC_FCMTLR0_H

-- 指定新数据日期分区位置
ALTER TABLE EXT_TODDC_FCMTLR0_H DROP IF EXISTS PARTITION(LOAD_DATE='${log_date}');
ALTER TABLE EXT_TODDC_FCMTLR0_H ADD PARTITION (LOAD_DATE='${log_date}') LOCATION 'hdfs://hacluster/bigdata/input/TS150/case_trace/TODDC_FCMTLR0_H/${log_date}/';

-- 备份贴源数据到ORC内部表
INSERT OVERWRITE TABLE INN_TODDC_FCMTLR0_H PARTITION(LOAD_DATE='${log_date}')
SELECT 
   -- 操作员号
   CM_OPR_NO,
   -- 时间戳
   CMTLR_DB_TIMESTAMP,
   -- 操作员姓名
   CM_OPR_NAME,
   -- 身份证号码
   CM_ID_NO,
   -- 柜员备注
   CM_TLR_RMRK,
   -- 作业等级
   CM_OPN_RANG,
   -- 密码
   CM_OPR_PSWD,
   -- 密码最后更正日期
   regexp_replace(CM_PSWD_FINAL_UPDT_DT, '-', ''),
   -- 通汇免授权金额
   CM_CORS_FAUTH_AMT,
   -- 最近作业日
   regexp_replace(CM_LTST_OPN_DT, '-', ''),
   -- 签到注记
   CM_SGNI_RMRK,
   -- 本日签到时间
   CM_CRNT_DAY_SGNI_TM,
   -- 本日签退时间
   CM_CRNT_DAY_SGNO_TM,
   -- 资料最后更正日期
   regexp_replace(CM_DATA_LST_UPDT_DT, '-', ''),
   -- 柜员状态
   CM_TLR_STS,
   -- 操作卡号
   CM_OPR_CRD_NO,
   -- 可执行交易组
   CM_EXCS_TX_SECN,
   -- 空白
   FILLER1,
   -- 柜员建档日期
   regexp_replace(CM_TLR_FLST_DT, '-', ''),
   -- 柜员签到错误次数
   CM_TLR_SG1_ERR_TM,
   -- 密钥代码
   CM_TSTKY_COD,
   -- 押码密钥
   CM_TEST_KEY,
   -- 柜员当日帐务流水序号
   CM_CUR_TX_SRL,
   -- 柜员次日帐务流水序号
   CM_NEXT_TX_SRL,
   -- 柜员当日非帐务流水序号
   CM_CUR_NON_TX_SRL,
   -- 柜员次日非帐务流水序号
   CM_NEXT_NON_TX_SRL,
   -- 预留流水笔数
   CM_REV_TX_NUMBER,
   -- 操作员卡号2
   CM_OPR_CRD_NO2,
   -- 前置柜员签到错误次数
   CM_FRONT_TLR_SG1_ERR_TM,
   -- 金库柜员标志
   CM_TLR_CASH_FLAG,
   -- 现金管理员标志
   CM_TLR_RNG_CSHR_FLG,
   -- 原始等级
   CM_OPN_RANG_INIT,
   -- 转出柜员号
   CM_OUT_TLR_OPR,
   -- 转出授权标志
   CM_OUT_AUTH_FLG,
   -- 转出生效日期
   regexp_replace(CM_OUT_AVL_DATE, '-', ''),
   -- 转出失效日期
   regexp_replace(CM_OUT_INAVL_DATE, '-', ''),
   -- 转入柜员号
   CM_IN_TLR_OPR,
   -- 转入授权标志
   CM_IN_AUTH_FLG,
   -- 转入生效日期
   regexp_replace(CM_IN_AVL_DATE, '-', ''),
   -- 转入失效日期
   regexp_replace(CM_IN_UNAVL_DATE, '-', ''),
   -- 所属机构号
   CM_OPUN_CODE,
   -- 柜员流水子码
   CM_SEQ_NO,
   -- 尾箱号
   CM_CSHBX_NO,
   -- 证券业务范围
   CM_CCBSS_TELLER_GRAD,
   -- 用户类型
   CM_USER_TYPE,
   -- #FILLER
   FILLER,
   -- P9开始日期
   regexp_replace(P9_START_DATE, '-', ''),
   -- P9结束日期
   regexp_replace(P9_END_DATE, '-', '')
  FROM EXT_TODDC_FCMTLR0_H
 WHERE LOAD_DATE='${log_date}';
