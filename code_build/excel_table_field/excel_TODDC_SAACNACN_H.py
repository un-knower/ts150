#field_en, field_cn, field_type, field_length, field_is_pk, field_is_dk, field_to_ctbase, index_describe, index_function, index_split, field_filter
table_cn = '个人活期存款主档'
field_array = [
    ('SAACN_LL', 'SAACN_LL', '', ''),
    ('SA_ACCT_NO', '帐号', 'Y', 'n'),
    ('SAACN_DB_TIMESTAMP', 'SAACN_DB_TIMESTAMP', '', ''),
    ('SA_PSBK_NO', '活存册号', '', ''),
    ('SA_PSBK_DL_DT', '存折挂失日期', '', ''),
    ('SA_TODAY_CSH_RMB', '当日累计取现金额（人民币）', '', ''),
    ('SA_RISK_LEVEL', '风险等级', '', ''),
    ('SA_PSBK_STS', '存折状态', '', ''),
    ('SA_MBSTMT_FLG', '寄对帐单标志', '', ''),
    ('SA_OPAC_INSTN_NO', '经营管理机构', 'Y', 'n'),
    ('SA_OPAC_AMT', '开户金额', 'Y', 'n'),
    ('SA_OPAC_DT', '开户日期', 'Y', 'n'),
    ('SA_RCRD_INSTN_NO', '记账机构', 'Y', ''),
    ('SA_ACCT_ORG_CTL_STS', '原账户控制状态', '', ''),
    ('SA_YBK_FLG', '医保卡标志', '', ''),
    ('SA_ACCT_CHAR_FX', '个人外币账户性质', '', ''),
    ('SA_CUST_NO', '客户编号', 'Y', 'n'),
    ('SA_CUST_NAME', '客户名称', 'Y', 'n'),
    ('SA_CONNTR_NO', '联系人编号', 'Y', 'n'),
    ('SA_CRPT_PIN', '密码', '', ''),
    ('SA_PSWD_DL_DT', '密码挂失日期', '', ''),
    ('SA_PSWD_STS', '密码状态', '', ''),
    ('SA_DET_ITEM_N', '明细笔数', '', ''),
    ('SA_CACCT_INSTN_NO', '销户机构号', 'Y', 'n'),
    ('SA_CACCT_DT', '销户日期', 'Y', 'n'),
    ('SA_PGLN_TOTL', '页行数', '', ''),
    ('SA_AENTR_DET_TOTL', '已登折明细数', '', ''),
    ('SA_ACKAC_DET_TOTL', '已对帐明细数', '', ''),
    ('SA_SLCRD_NO', '印鉴卡编号', '', ''),
    ('SA_PRDS_INSTN_DPDW_FLG', '约定机构存取标志', '', ''),
    ('SA_ACCT_TYP', '帐别', '', ''),
    ('SA_ACCT_CHAR', '帐户性质', '', ''),
    ('SA_DRW_TYP', '支取方式', '', ''),
    ('SA_ASTMT_ADDR_COD', '对帐单地址代码', '', ''),
    ('SA_SEAL_STS', '印鉴状态', '', ''),
    ('SA_ACC_PAGE_NO', '帐簿当前页', '', ''),
    ('SA_PRINTED_MAX_NO', '已打印最大序号', '', ''),
    ('SA_ACC_UNPRINT_NO', '未打印帐簿笔数', '', ''),
    ('SA_OPAC_TL_NO', '开户柜员号', 'Y', 'n'),
    ('SA_CACCT_TL_NO', '销户柜员号', 'Y', 'n'),
    ('SA_INTC_FLG', '计息标志', '', ''),
    ('SA_SEAL_DL_DT', '印鉴挂失日期', '', ''),
    ('SA_STLM_SVC_STY', '结算手续费收取方式', '', ''),
    ('SA_DEP_TYP', '贷款种类', '', ''),
    ('SA_PAGESUM_N', '页数', '', ''),
    ('SA_CURR_CHAR', '钞汇属性', '', ''),
    ('SA_AVAL_DT', '起用日期', '', ''),
    ('SA_XT_SIGN_FLG', '外汇买卖质押炒汇签约标志', '', ''),
    ('SA_YHT_SIGN_FLG', '理财卡一户通备付金签约标志', 'Y', 'n'),
    ('SA_XYK_FLG', '校园卡标志', 'Y', 'n'),
    ('SA_LMT_CTL_FLG', '支付限额控制标志', 'Y', 'n'),
    ('SA_VIR_ACCT_FLG', '集团客户虚账户标志', '', ''),
    ('SA_GRP_SIGN_FLG', '集团现金池签约标志', '', ''),
    ('SA_XT_ACCT_FLG', '外汇保证金帐户标志', '', ''),
    ('SA_PSWD_ERR_TIMES', '密码出错次数', '', ''),
    ('SA_TODAY_CSH_USD', '当日累计取现金额（美金）', '', ''),
    ('SA_CARD_NO', '卡号', 'Y', 'n'),
    ('SA_INT_TAX_FLG', '利息税计税标志', '', ''),
    ('SA_ACCT_CTL_STS', '帐户控制状态', '', ''),
    ('SA_FS_STS', '综合理财标志', 'Y', 'n'),
    ('SA_NEXT_PAGE_FLG_N', '承上页标志', '', ''),
    ('SA_SIGN_FLG', '签约标志', '', ''),
    ('SA_CCR_FLG', '是否贷记卡指定还款帐号', 'Y', 'n'),
    ('SA_LAST_TXN_DT', '上次明细日', '', ''),
    ('SA_DPDW_RANG', '通兑范围', '', ''),
    ('SA_RECOG_TYP_NUM_N', '关注种类个数', '', ''),
    ('SA_ORG_DEP_TYPE', '原存款种类', '', ''),
    ('SA_LAST_CSH_DT', '最后取现日期', '', ''),
    ('SA_GG_ACCT_FLG', '共管帐户标志', '', ''),
    ('SA_NON_FRZ_CONSIGN_TIME', '非冻结委托次数', '', ''),
    ('SA_DIST_COD', '行政区域代码', '', ''),
    ('SA_PSBK_PRT_NO1', '存折印刷号1', '', ''),
    ('P9_SPLIT_BRANCH_CD', '按机构拆分字段', '', ''),
    ('P9_START_DATE', 'P9开始日期', 'Y', ''),
    ('P9_START_BATCH', '', '', ''),
    ('P9_END_DATE', 'P9结束日期', 'Y', ''),
    ('P9_END_BATCH', '', '', ''),
    ('P9_DEL_FLAG', '', '', ''),
    ('P9_JOB_NAME', '', '', ''),
    ('SA_TSF_FLG', '假名账户标志', '', ''),
    ('SA_FAMILY_FLG', '家庭现金管理标志', '', ''),
]