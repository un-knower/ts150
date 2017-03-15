use sor;

# 中间表插入
# 对私客户信息整合表，带原系统客户号: MID_CUSTOMER_CUR

-- 有关联的数据插入
INSERT OVERWRITE TABLE MID_CUSTOMER_CUR
SELECT
   -- #源信息系统代码
   SRC_INF_STM_CD,
   -- #源系统客户编号
   SRCSYS_CST_ID,
   -- 客户编号
   CST_ID,
   -- 客户编号
   CST_ID,
   -- 个人法定名称
   IDV_LGL_NM,
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
   -- 最佳联系电话
   BEST_CTC_TEL,
   -- 企业高级管理人员标志
   ENTP_ADV_MGTPPL_IND,
   -- 企业实际控制人标志
   ENTP_ACT_CTRL_PSN_IND,
   -- 企业法人标志
   ENLGPS_IND,
   -- 非居民标志
   NON_RSDNT_IND,
   -- 工作单位性质代码
   WRK_UNIT_CHAR_CD,
   -- #源信息系统代码
   SRC_INF_STM_CD,
   -- #源系统客户编号
   SRCSYS_CST_ID,
   -- 客户编号
   CST_ID,
   -- 客户编号
   CST_ID,
   -- 个人法定名称
   IDV_LGL_NM,
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
   -- 最佳联系电话
   BEST_CTC_TEL,
   -- 企业高级管理人员标志
   ENTP_ADV_MGTPPL_IND,
   -- 企业实际控制人标志
   ENTP_ACT_CTRL_PSN_IND,
   -- 企业法人标志
   ENLGPS_IND,
   -- 非居民标志
   NON_RSDNT_IND,
   -- 工作单位性质代码
   WRK_UNIT_CHAR_CD FROM
(SELECT
        -- #源信息系统代码
        SRC_INF_STM_CD,
        -- #源系统客户编号
        SRCSYS_CST_ID,
        -- 客户编号
        CST_ID
   FROM CT_T0042_TBPC9030_H
  WHERE P9_END_DATE = '29991231'
    AND (SRC_INF_STM_CD is not null AND SRC_INF_STM_CD <> '')
   AND (SRCSYS_CST_ID is not null AND SRCSYS_CST_ID <> '')) t
INNER JOIN
(SELECT
        -- 客户编号
        CST_ID,
        -- 个人法定名称
        IDV_LGL_NM,
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
        -- 最佳联系电话
        BEST_CTC_TEL,
        -- 企业高级管理人员标志
        ENTP_ADV_MGTPPL_IND,
        -- 企业实际控制人标志
        ENTP_ACT_CTRL_PSN_IND,
        -- 企业法人标志
        ENLGPS_IND,
        -- 非居民标志
        NON_RSDNT_IND,
        -- 工作单位性质代码
        WRK_UNIT_CHAR_CD
   FROM CT_T0042_TBPC1010_H
  WHERE P9_END_DATE = '29991231'
    AND (CST_ID is not null AND CST_ID <> '')) t

-- 追加从表为空的记录
INSERT INTO TABLE MID_CUSTOMER_CUR
SELECT
        SRC_INF_STM_CD,
        SRCSYS_CST_ID,
        CST_ID,
        null as CST_ID,
        null as IDV_LGL_NM,
        null as CRDT_TPCD,
        null as CRDT_NO,
        null as BRTH_DT,
        null as GND_CD,
        null as NAT_CD,
        null as RSDNC_NAT_CD,
        null as PREF_LNG_CD,
        null as HSHLDRGST_ADIV_CD,
        null as MAR_STTN_CD,
        null as CHL_STTN_CD,
        null as ETHNCT_CD,
        null as RLG_CD,
        null as PLTCLPARTY_CD,
        null as LCS_CD,
        null as BLNG_INSID,
        null as CCB_EMPE_IND,
        null as PLN_FNC_EFCT_IND,
        null as IMPT_PSNG_IND,
        null as PTNL_VIP_IND,
        null as SPCLVIP_IND,
        null as STM_EVL_CST_GRD_CD,
        null as MNUL_EVL_CST_GRD_CD,
        null as PRVT_BNK_CST_GRD_CD,
        null as PRVT_BNK_SIGN_CST_IND,
        null as MO_INCMAM,
        null as CSTMGR_ID,
        null as BEST_CTC_TEL,
        null as ENTP_ADV_MGTPPL_IND,
        null as ENTP_ACT_CTRL_PSN_IND,
        null as ENLGPS_IND,
        null as NON_RSDNT_IND,
        null as WRK_UNIT_CHAR_CD 
  FROM T0042_TBPC1010_H
 WHERE P9_END_DATE = '29991231'
   AND (SRC_INF_STM_CD is null OR SRC_INF_STM_CD = '')
   AND (SRCSYS_CST_ID is null OR SRCSYS_CST_ID = '');
