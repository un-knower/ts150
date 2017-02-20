use sor;

# Hive贴源数据处理
# 客户基本信息: T0042_TBPC1010_H

-- 指定新数据日期分区位置
ALTER TABLE EXT_T0042_TBPC1010_H DROP IF EXISTS PARTITION(LOAD_DATE='${log_date}');
ALTER TABLE EXT_T0042_TBPC1010_H ADD PARTITION (LOAD_DATE='${log_date}') LOCATION 'hdfs://hacluster/bigdata/input/case_trace/T0042_TBPC1010_H/${log_date}/';

-- 备份贴源数据到ORC内部表
INSERT OVERWRITE TABLE INN_T0042_TBPC1010_H PARTITION(LOAD_DATE='${log_date}')
SELECT 
   -- 客户编号
   CST_ID,
   -- #记录失效时间戳
   RCRD_EXPY_TMS,
   -- #多实体标识
   MULTI_TENANCY_ID,
   -- 个人法定名称
   IDV_LGL_NM,
   -- 个人姓名拼音全称
   IDV_NM_CPA_FULLNM,
   -- 证件类型代码
   CRDT_TPCD,
   -- 证件号码
   CRDT_NO,
   -- 出生日期
   BRTH_DT,
   -- 性别代码
   GND_CD,
   -- 国籍代码
   NAT_CD,
   -- 居住国代码
   RSDNC_NAT_CD,
   -- 首选语言代码
   PREF_LNG_CD,
   -- 户籍行政区划代码
   HSHLDRGST_ADIV_CD,
   -- 婚姻状况代码
   MAR_STTN_CD,
   -- 子女状况代码
   CHL_STTN_CD,
   -- 民族代码
   ETHNCT_CD,
   -- 宗教信仰代码
   RLG_CD,
   -- 政治面貌代码
   PLTCLPARTY_CD,
   -- 生命周期状态代码
   LCS_CD,
   -- 所属机构编号
   BLNG_INSID,
   -- 建行员工标志
   CCB_EMPE_IND,
   -- 计划财务有效客户标志
   PLN_FNC_EFCT_IND,
   -- 重要人士标志
   IMPT_PSNG_IND,
   -- 潜力VIP标志
   PTNL_VIP_IND,
   -- 特殊vip标志
   SPCLVIP_IND,
   -- 系统评定客户等级代码
   STM_EVL_CST_GRD_CD,
   -- 手工评定客户等级代码
   MNUL_EVL_CST_GRD_CD,
   -- 私人银行客户等级代码
   PRVT_BNK_CST_GRD_CD,
   -- 私人银行签约客户标志
   PRVT_BNK_SIGN_CST_IND,
   -- 月收入金额
   MO_INCMAM,
   -- 客户经理编号
   CSTMGR_ID,
   -- 最佳联系时间代码
   BEST_CTC_TM_CD,
   -- 最佳联系方式代码
   BEST_CTC_MTDCD,
   -- 特殊名单标志
   SPNMLT_IND,
   -- 关联方标志
   RELPARTY_IND,
   -- 监管认定关联方标志
   REG_AFM_RELPARTY_IND,
   -- 电话类型代码
   TEL_TPCD,
   -- 最佳联系电话
   BEST_CTC_TEL,
   -- 一级分行号
   LV1_BR_NO,
   -- 偏好寄送方式代码
   PREF_MSND_MTDCD,
   -- 接收邮寄地址类型代码
   RCV_MAIL_ADR_TPCD,
   -- 企业高级管理人员标志
   ENTP_ADV_MGTPPL_IND,
   -- 企业实际控制人标志
   ENTP_ACT_CTRL_PSN_IND,
   -- 企业法人标志
   ENLGPS_IND,
   -- 客户渠道商机标志
   CST_CHNL_BSOP_IND,
   -- #员工渠道商机标志
   EMPCHNL_BSOP_IND,
   -- 个人客户AUM余额
   IDCST_AUM_BAL,
   -- #多实体标志
   MT_ENT_IND,
   -- 非居民标志
   NON_RSDNT_IND,
   -- 工作单位性质代码
   WRK_UNIT_CHAR_CD,
   -- 工作单位名称
   WRK_UNIT_NM,
   -- #备用字段1信息
   RSRV_FLD1_INF,
   -- #备用字段2信息
   RSRV_FLD2_INF,
   -- #备用字段3信息
   RSRV_FLD3_INF,
   -- 创建机构编号
   CRT_INSID,
   -- 创建员工编号
   CRT_EMPID,
   -- TODDC_CRATMDET_A
   LAST_UDT_INSID,
   -- TODDC_CRATMDET_A
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
  FROM EXT_T0042_TBPC1010_H
 WHERE LOAD_DATE='${log_date}';
