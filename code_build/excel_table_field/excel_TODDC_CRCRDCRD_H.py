#field_en, field_cn, field_type, field_length, field_is_pk, field_is_dk, field_to_ctbase, index_describe, index_function, index_split, field_filter
table_cn = '银行卡主档'
field_array = [
    ('CRCRD_LL', '(空值)', '', ''),
    ('CR_CRD_NO', '卡号', 'Y', 'n'),
    ('CRCRD_DB_TIMESTAMP', '(空值)', '', ''),
    ('CR_RSV_FLG_1', '备用标志1;员工标志', '', ''),
    ('CR_RSV_FLG_2', '备用标志2;金卡标志', '', ''),
    ('CR_RSV_FLG_3', '备用标志3;[信用/储蓄]卡标志', '', ''),
    ('CR_RSV_FLG_4', '备用标志4;带折标志', '', ''),
    ('CR_RSV_FLG_5', '备用标志5;综合理财标志', '', ''),
    ('CR_RSV_FLG_6', '备用标志6;发卡标志', '', ''),
    ('CR_RSV_FLG_7', '备用标志7;级别标志', '', ''),
    ('CR_RSV_FLG_8', '备用标志8', '', ''),
    ('R_COLOR_PHOTO_FLAG', '彩照标志', '', ''),
    ('CR_UNIT_CRD_FLG', '单位卡标志', '', ''),
    ('CR_UNIT_CUST_NO', '单位客户编号', '', ''),
    ('CRNT_DT_ATM_DRW_TM', '当日ATM取款次数', '', ''),
    ('RNT_DT_ATM_DRW_AMT', '当日ATM取款金额', '', ''),
    ('CR_DL_DT', '挂失日期', 'Y', 'n'),
    ('CR_DL_STS', '挂失状态', 'Y', 'n'),
    ('CR_DR_CR_COD', '借贷别', 'Y', ''),
    ('CR_CRD_TYP_COD', '卡种代码', 'Y', 'n'),
    ('CR_CRD_STS', '卡状态', 'Y', 'n'),
    ('CR_CUST_NO', '客户编号', 'Y', 'n'),
    ('CR_JONT_COD', '联名代码', '', ''),
    ('CR_CNCL_STS1', '注销状态一;银行主动注销', '', 'n'),
    ('CR_CNCL_STS2', '注销状态二;主卡注销附属卡', '', 'n'),
    ('CR_CNCL_STS3', '注销状态三;单位注销单位卡', '', 'n'),
    ('CR_CNCL_STS4', '注销状态四;担保人撤保', '', 'n'),
    ('CR_CNCL_STS5', '注销状态五;主卡申请注销', '', 'n'),
    ('CR_CNCL_STS6', '注销状态六;系统自动止付', '', 'n'),
    ('CR_AGRE_COD', '认同代码', '', ''),
    ('CR_ACCD_SQ_NO', '事故序号', '', ''),
    ('CR_COLC_FLG1', '收费标志1', '', 'n'),
    ('CR_COLC_FLG2', '收费标志2', '', 'n'),
    ('CR_COLC_FLG3', '收费标志3', '', 'n'),
    ('CR_COLC_FLG4', '收费标志4', '', 'n'),
    ('CR_COLC_FLG5', '收费标志5', '', 'n'),
    ('CR_COLC_FLG6', '收费标志6', '', 'n'),
    ('CR_COLC_FLG7', '收费标志7;临时额度标志', '', 'n'),
    ('CR_COLC_FLG8', '收费标志8;银联标志', '', 'n'),
    ('CR_CRDG', '信用等级', '', ''),
    ('CR_DELAY_STS', '延期状态', '', ''),
    ('CR_ANFE_CHG_PCT', '年费收取比例', '', 'n'),
    ('CR_ANFE_REV_AMT', '年费已扣收金额', '', 'n'),
    ('CR_ANFE_YEAR', '年费收取年份', '', 'n'),
    ('ANFE_UNPAY_MONTHS', '年费未扣收月数', '', 'n'),
    ('CR_CVV2', 'CVV2', '', ''),
    ('CR_FILLER_CRD', '备注', '', ''),
    ('CR_OPUN_COD', '营业单位代码', 'Y', 'n'),
    ('CR_EXPD_DT', '有效期限', '', ''),
    ('CR_CRDMD_STS', '制卡状态', '', ''),
    ('CR_MNSUB_DRP', '主附卡标志', 'Y', 'n'),
    ('CR_PCRD_NO', '主卡卡号', 'Y', 'n'),
    ('CR_CNCLC_DT', '注销日期', 'Y', 'n'),
    ('CR_CNCL_STS', '注销状态', 'Y', 'n'),
    ('CR_SPUS_COD', '专用代码', '', ''),
    ('CR_VRSN_NO', '版本号', '', ''),
    ('R_CRD_COST_REV_RTO', '卡片工本费收取比例', '', ''),
    ('CR_AGENT_PAY_FLG', '代发工资标志', '', ''),
    ('CR_ATM_LTM_TX_DT', 'ATM上次交易日期', '', ''),
    ('CR_CUST_APLY_NO', '客户申请编号', '', ''),
    ('CR_CHG_GRNTR_DT', '更改日期', 'Y', 'n'),
    ('CR_OPCR_DATE', '开卡日期', 'Y', 'n'),
    ('CRNT_DT_ATM_TRN_TM', '当日ATM转帐次数', '', ''),
    ('RNT_DT_ATM_TRN_AMT', '当日ATM转帐金额', '', ''),
    ('CRNT_DT_TB_TRN_TM', '当日他行转帐次数', '', ''),
    ('CRNT_DT_TB_TRN_AMT', '当日他行转帐金额', '', ''),
    ('CR_STPMT_DATE', '止付日期', '', ''),
    ('CR_APP_NO', '发卡机构号', 'Y', 'n'),
    ('CRNT_FX_ATM_DRW_TM', '当日外币ATM取款次数', '', ''),
    ('RNT_FX_AMT_DRW_AMT', '当日外币ATM取款金额', '', ''),
    ('CR_PSWD', '密码', '', ''),
    ('CR_PSWD_CNT', '密码次数', '', ''),
    ('CR_SEC_MT_CON', '二磁道', '', ''),
    ('CR_SEC_MT_CON_CNT', '二磁道校验次数', '', ''),
    ('CR_POS_CHK_FLG', 'POS密码检查标志', '', ''),
    ('CR_IC_PCRD_NO', 'IC卡主帐户', '', ''),
    ('CR_CRD_CHG_DT', '换卡日期', 'Y', 'n'),
    ('CR_IC_CNT', 'IC卡数量', '', ''),
    ('CR_CHG_OPUN_COD', '换卡机构号', 'Y', 'n'),
    ('HK_INVALI_DT_TIMES', '失效日期出错次数', '', ''),
    ('FILLER', 'FILLER', '', ''),
    ('P9_SPLIT_BRANCH_CD', '按机构拆分字段', '', ''),
    ('P9_START_DATE', 'P9开始日期', 'Y', ''),
    ('P9_START_BATCH', '', '', ''),
    ('P9_END_DATE', 'P9结束日期', 'Y', ''),
    ('P9_END_BATCH', '', '', ''),
    ('P9_DEL_FLAG', '', '', ''),
    ('P9_JOB_NAME', '', '', ''),
]
