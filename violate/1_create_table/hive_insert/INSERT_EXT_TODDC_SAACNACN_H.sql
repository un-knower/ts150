use sor;

-- Hive贴源数据处理
-- 个人活期存款主档: TODDC_SAACNACN_H

-- 指定新数据日期分区位置
ALTER TABLE EXT_TODDC_SAACNACN_H DROP IF EXISTS PARTITION(LOAD_DATE='${log_date}');
ALTER TABLE EXT_TODDC_SAACNACN_H ADD PARTITION (LOAD_DATE='${log_date}') LOCATION 'hdfs://hacluster/bigdata/input/TS150/case_trace/TODDC_SAACNACN_H/${log_date}/';

-- 备份贴源数据到ORC内部表
INSERT OVERWRITE TABLE INN_TODDC_SAACNACN_H PARTITION(LOAD_DATE='${log_date}')
SELECT 
   -- SAACN_LL
   SAACN_LL,
   -- 帐号
   SA_ACCT_NO,
   -- SAACN_DB_TIMESTAMP
   SAACN_DB_TIMESTAMP,
   -- 活存册号
   SA_PSBK_NO,
   -- 存折挂失日期
   regexp_replace(SA_PSBK_DL_DT, '-', ''),
   -- 当日累计取现金额（人民币）
   SA_TODAY_CSH_RMB,
   -- 风险等级
   SA_RISK_LEVEL,
   -- 存折状态
   SA_PSBK_STS,
   -- 寄对帐单标志
   SA_MBSTMT_FLG,
   -- 经营管理机构
   SA_OPAC_INSTN_NO,
   -- 开户金额
   SA_OPAC_AMT,
   -- 开户日期
   regexp_replace(SA_OPAC_DT, '-', ''),
   -- 记账机构
   SA_RCRD_INSTN_NO,
   -- 原账户控制状态
   SA_ACCT_ORG_CTL_STS,
   -- 医保卡标志
   SA_YBK_FLG,
   -- 个人外币账户性质
   SA_ACCT_CHAR_FX,
   -- 客户编号
   SA_CUST_NO,
   -- 客户名称
   SA_CUST_NAME,
   -- 联系人编号
   SA_CONNTR_NO,
   -- 密码
   SA_CRPT_PIN,
   -- 密码挂失日期
   regexp_replace(SA_PSWD_DL_DT, '-', ''),
   -- 密码状态
   SA_PSWD_STS,
   -- 明细笔数
   SA_DET_ITEM_N,
   -- 销户机构号
   SA_CACCT_INSTN_NO,
   -- 销户日期
   regexp_replace(SA_CACCT_DT, '-', ''),
   -- 页行数
   SA_PGLN_TOTL,
   -- 已登折明细数
   SA_AENTR_DET_TOTL,
   -- 已对帐明细数
   SA_ACKAC_DET_TOTL,
   -- 印鉴卡编号
   SA_SLCRD_NO,
   -- 约定机构存取标志
   SA_PRDS_INSTN_DPDW_FLG,
   -- 帐别
   SA_ACCT_TYP,
   -- 帐户性质
   SA_ACCT_CHAR,
   -- 支取方式
   SA_DRW_TYP,
   -- 对帐单地址代码
   SA_ASTMT_ADDR_COD,
   -- 印鉴状态
   SA_SEAL_STS,
   -- 帐簿当前页
   SA_ACC_PAGE_NO,
   -- 已打印最大序号
   SA_PRINTED_MAX_NO,
   -- 未打印帐簿笔数
   SA_ACC_UNPRINT_NO,
   -- 开户柜员号
   SA_OPAC_TL_NO,
   -- 销户柜员号
   SA_CACCT_TL_NO,
   -- 计息标志
   SA_INTC_FLG,
   -- 印鉴挂失日期
   regexp_replace(SA_SEAL_DL_DT, '-', ''),
   -- 结算手续费收取方式
   SA_STLM_SVC_STY,
   -- 贷款种类
   SA_DEP_TYP,
   -- 页数
   SA_PAGESUM_N,
   -- 钞汇属性
   SA_CURR_CHAR,
   -- 起用日期
   regexp_replace(SA_AVAL_DT, '-', ''),
   -- 外汇买卖质押炒汇签约标志
   SA_XT_SIGN_FLG,
   -- 理财卡一户通备付金签约标志
   SA_YHT_SIGN_FLG,
   -- 校园卡标志
   SA_XYK_FLG,
   -- 支付限额控制标志
   SA_LMT_CTL_FLG,
   -- 集团客户虚账户标志
   SA_VIR_ACCT_FLG,
   -- 集团现金池签约标志
   SA_GRP_SIGN_FLG,
   -- 外汇保证金帐户标志
   SA_XT_ACCT_FLG,
   -- 密码出错次数
   SA_PSWD_ERR_TIMES,
   -- 当日累计取现金额（美金）
   SA_TODAY_CSH_USD,
   -- 卡号
   SA_CARD_NO,
   -- 利息税计税标志
   SA_INT_TAX_FLG,
   -- 帐户控制状态
   SA_ACCT_CTL_STS,
   -- 综合理财标志
   SA_FS_STS,
   -- 承上页标志
   SA_NEXT_PAGE_FLG_N,
   -- 签约标志
   SA_SIGN_FLG,
   -- 是否贷记卡指定还款帐号
   SA_CCR_FLG,
   -- 上次明细日
   SA_LAST_TXN_DT,
   -- 通兑范围
   SA_DPDW_RANG,
   -- 关注种类个数
   SA_RECOG_TYP_NUM_N,
   -- 原存款种类
   SA_ORG_DEP_TYPE,
   -- 最后取现日期
   SA_LAST_CSH_DT,
   -- 共管帐户标志
   SA_GG_ACCT_FLG,
   -- 非冻结委托次数
   SA_NON_FRZ_CONSIGN_TIME,
   -- 行政区域代码
   SA_DIST_COD,
   -- 存折印刷号1
   SA_PSBK_PRT_NO1,
   -- P9开始日期
   regexp_replace(P9_START_DATE, '-', ''),
   -- P9结束日期
   regexp_replace(P9_END_DATE, '-', ''),
   -- 假名账户标志
   SA_TSF_FLG,
   -- 家庭现金管理标志
   SA_FAMILY_FLG
  FROM EXT_TODDC_SAACNACN_H
 WHERE LOAD_DATE='${log_date}';
