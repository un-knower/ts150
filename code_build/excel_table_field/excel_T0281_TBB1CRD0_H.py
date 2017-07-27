#field_en, field_cn, field_type, field_length, field_is_pk, field_is_dk, field_to_ctbase, index_describe, index_function, index_split, field_filter
table_cn = '信用卡合约账户'
field_array = [
    ('SHLDREPYMTHGST_BAL_DT', '应还款最高余额日期', '', ''),
    ('CRCRD_ACC_LAST_MNT_DT', '信用卡账户最后维护日期', '', ''),
    ('CRCRDINTRTLAST_MNT_DT', '信用卡利率最后维护日期', '', ''),
    ('UDT_SHLDREPYMT_AMT_DT', '更新应还款额日期', '', ''),
    ('UPGRD_ACC_EFDT', '升等账户生效日期', '', ''),
    ('LSTTMGENLATEPYMTAMTDT', '上次产生滞纳金额日期', '', ''),
    ('CNCLAFVERF_ST_CHG_DT', '核销状态变化日期', '', ''),
    ('CR_BAL_RET_DT', '贷记余额返还日期', '', ''),
    ('ACC_RCRD_BLCNO_DT', '账户记录封锁码日期', '', ''),
    ('ACCRCRD_PREV_BLCNO_DT', '账户记录上一封锁码日期', '', ''),
    ('ACG_ST_UDT_DT', '账务状态更新日期', '', ''),
    ('CRCRDLSTTMRYACCENTRDT', '信用卡上次还款入账日期', '', ''),
    ('CRCRD_CRLINE', '信用卡信用额度', '', ''),
    ('CRCRD_LSTTM_CRLINE', '信用卡上次信用额度', '', ''),
    ('CRCRD_ACC_HGST_BAL', '信用卡账户最高余额', '', ''),
    ('SHLDREPYMT_HGST_BAL', '应还款最高余额', '', ''),
    ('CRCRDACCDBT_BAL_MONUM', '信用卡账户借记余额月数', '', ''),
    ('CRCRDACCENCMTBALMONUM', '信用卡账户取现余额月数', '', ''),
    ('ODUE_HIST_ST_TPCD_1', '第一逾期历史状态类型代码', '', ''),
    ('ODUE_HIST_ST_TPCD_2', '第二逾期历史状态类型代码', '', ''),
    ('ODUE_HIST_ST_TPCD_3', '第三逾期历史状态类型代码', '', ''),
    ('ODUE_HIST_ST_TPCD_4', '第四逾期历史状态类型代码', '', ''),
    ('ODUE_HIST_ST_TPCD_5', '第五逾期历史状态类型代码', '', ''),
    ('ODUE_HIST_ST_TPCD_6', '第六逾期历史状态类型代码', '', ''),
    ('ODUE_HIST_ST_TPCD_7', '第七逾期历史状态类型代码', '', ''),
    ('ODUE_HIST_ST_TPCD_8', '第八逾期历史状态类型代码', '', ''),
    ('ODUE_HIST_ST_TPCD_9', '第九逾期历史状态类型代码', '', ''),
    ('ODUE_HIST_ST_TPCD_10', '第十逾期历史状态类型代码', '', ''),
    ('ODUE_HIST_ST_TPCD_11', '第十一逾期历史状态类型代码', '', ''),
    ('ODUE_HIST_ST_TPCD_12', '第十二逾期历史状态类型代码', '', ''),
    ('ODUE_HIST_ST_TPCD_13', '第十三逾期历史状态类型代码', '', ''),
    ('ODUE_HIST_ST_TPCD_14', '第十四逾期历史状态类型代码', '', ''),
    ('ODUE_HIST_ST_TPCD_15', '第十五逾期历史状态类型代码', '', ''),
    ('ODUE_HIST_ST_TPCD_16', '第十六逾期历史状态类型代码', '', ''),
    ('ODUE_HIST_ST_TPCD_17', '第十七逾期历史状态类型代码', '', ''),
    ('ODUE_HIST_ST_TPCD_18', '第十八逾期历史状态类型代码', '', ''),
    ('ODUE_HIST_ST_TPCD_19', '第十九逾期历史状态类型代码', '', ''),
    ('ODUE_HIST_ST_TPCD_20', '第二十逾期历史状态类型代码', '', ''),
    ('ODUE_HIST_ST_TPCD_21', '第二十一逾期历史状态类型代码', '', ''),
    ('ODUE_HIST_ST_TPCD_22', '第二十二逾期历史状态类型代码', '', ''),
    ('ODUE_HIST_ST_TPCD_23', '第二十三逾期历史状态类型代码', '', ''),
    ('ODUE_HIST_ST_TPCD_24', '第二十四逾期历史状态类型代码', '', ''),
    ('ODUHISBALSTNSUM_1', '第一逾期历史余额情况概述', '', ''),
    ('ODUHISBALSTNSUM_2', '第二逾期历史余额情况概述', '', ''),
    ('ODUHISBALSTNSUM_3', '第三逾期历史余额情况概述', '', ''),
    ('ODUHISBALSTNSUM_4', '第四逾期历史余额情况概述', '', ''),
    ('ODUHISBALSTNSUM_5', '第五逾期历史余额情况概述', '', ''),
    ('ODUHISBALSTNSUM_6', '第六逾期历史余额情况概述', '', ''),
    ('ODUHISBALSTNSUM_7', '第七逾期历史余额情况概述', '', ''),
    ('ODUHISBALSTNSUM_8', '第八逾期历史余额情况概述', '', ''),
    ('ODUHISBALSTNSUM_9', '第九逾期历史余额情况概述', '', ''),
    ('ODUHISBALSTNSUM_10', '第十逾期历史余额情况概述', '', ''),
    ('ODUHISBALSTNSUM_11', '第十一逾期历史余额情况概述', '', ''),
    ('ODUHISBALSTNSUM_12', '第十二逾期历史余额情况概述', '', ''),
    ('ODUHISBALSTNSUM_13', '第十三逾期历史余额情况概述', '', ''),
    ('ODUHISBALSTNSUM_14', '第十四逾期历史余额情况概述', '', ''),
    ('ODUHISBALSTNSUM_15', '第十五逾期历史余额情况概述', '', ''),
    ('ODUHISBALSTNSUM_16', '第十六逾期历史余额情况概述', '', ''),
    ('ODUHISBALSTNSUM_17', '第十七逾期历史余额情况概述', '', ''),
    ('ODUHISBALSTNSUM_18', '第十八逾期历史余额情况概述', '', ''),
    ('ODUHISBALSTNSUM_19', '第十九逾期历史余额情况概述', '', ''),
    ('ODUHISBALSTNSUM_20', '第二十逾期历史余额情况概述', '', ''),
    ('ODUHISBALSTNSUM_21', '第二十一逾期历史余额情况概述', '', ''),
    ('ODUHISBALSTNSUM_22', '第二十二逾期历史余额情况概述', '', ''),
    ('ODUHISBALSTNSUM_23', '第二十三逾期历史余额情况概述', '', ''),
    ('ODUHISBALSTNSUM_24', '第二十四逾期历史余额情况概述', '', ''),
    ('EXCDLMT_IND', '超限标志', '', ''),
    ('CRCRD_EXCDLMT_BAL', '信用卡超限余额', '', ''),
    ('CRCRD_CNCLAFVERF_RSCD', '信用卡核销原因代码', '', ''),
    ('CRCRDCNCLAVRNSPLMTDSC', '信用卡核销原因补充描述', '', ''),
    ('CRCRD_ACC_USE_STCD', '信用卡账户使用状态代码', '', ''),
    ('CST_CRCRD_CRDISU_DNUM', '客户信用卡发卡笔数', '', ''),
    ('DLAY_REPY_TPCD', '延迟还款类型代码', '', ''),
    ('LWSTREPY_AMT_CLMTD_CD', '最低还款金额计算方式代码', '', ''),
    ('APTRPY_MTDCD', '约定还款方式代码', '', ''),
    ('FIX_REPY_AMT', '固定还款金额', '', ''),
    ('PREVCRCRDACCODST_TPCD', '上一信用卡账户逾期状态类型代码', '', ''),
    ('CRCRD_ACC_ODST_TPCD', '信用卡账户逾期状态类型代码', '', ''),
    ('CRCRD_ADVRPY_AMT', '信用卡预先还款金额', '', ''),
    ('CRCRDACCLSHDREPYMTAMT', '信用卡账户最低应还款金额', '', ''),
    ('CRCRDRECLCLT_ODUE_IND', '信用卡重算逾期标志', '', ''),
    ('LSTTM_RECLCLT_ODUE_DT', '上次重算逾期日期', '', ''),
    ('NOT_ACCENTR_AHN_AMT', '未入账授权金额', '', ''),
    ('NOT_ACCENTR_AHN_DNUM', '未入账授权笔数', '', ''),
    ('CRCRD_AVL_LMT', '信用卡可用额度', '', ''),
    ('DLY_ENCSHMT_CRLINE', '每日取现信用额度', '', ''),
    ('DLYENCSHMT_AVL_CRLINE', '每日取现可用信用额度', '', ''),
    ('CRCRD_CR_INT_AMT', '信用卡贷方利息金额', '', ''),
    ('CRCRD_CR_INTAR_BAL', '信用卡贷方计息余额', '', ''),
    ('CRCRD_CR_ACM_INT_DYS', '信用卡贷方累计利息天数', '', ''),
    ('CRCRDCRNYR_CR_INT_AMT', '信用卡当年贷方利息金额', '', ''),
    ('CRCRDLASTYRCR_INT_AMT', '信用卡上年贷方利息金额', '', ''),
    ('CRCRDCRNYRCRINT_EPAMT', '信用卡当年贷方利息减免金额', '', ''),
    ('CRCRDLASTYRCRINTEPAMT', '信用卡上年贷方利息减免金额', '', ''),
    ('CRCRDCURFNCYRCRINTAMT', '信用卡当前财政年贷方利息金额', '', ''),
    ('CRCRDPREVFNCYCRINTAMT', '信用卡上一财政年贷方利息金额', '', ''),
    ('CRCRD_ACG_SETL_DT', '信用卡账务结算日期', '', ''),
    ('CRCRD_ORD_INSTM_LMT', '信用卡普通分期额度', '', ''),
    ('INSTM_NOT_PSTG_AMT', '分期未抛账金额', '', ''),
    ('INSTM_NOT_ACCENTRAMT', '分期未入账金额', '', ''),
    ('CRCRDORDINSTM_AVL_LMT', '信用卡普通分期可用额度', '', ''),
    ('SPINSTM_TAMT', '专项分期总额', '', ''),
    ('SPINSTMNTRCHD_AHN_AMT', '专项分期未达授权金额', '', ''),
    ('SPINSTM_NOT_PSTG_AMT', '专项分期未抛账金额', '', ''),
    ('INTDAYCRCRDBLINSTMAMT', '当日信用卡账单分期金额', '', ''),
    ('LNCRD_REPY_GRC_PRD', '贷记卡还款宽限期', '', ''),
    ('CRCRD_PRM_LMT', '信用卡永久额度', '', ''),
    ('CRCRD_TEMP_LMT', '信用卡临时额度', '', ''),
    ('TEMP_LMT_EFDT', '临时额度生效日期', '', ''),
    ('TEMP_LMT_EXPDT', '临时额度失效日期', '', ''),
    ('CRCRD_ENCSHMT_CRLINE', '信用卡取现信用额度', '', ''),
    ('CRCRDAVLENCSHMTCRLINE', '信用卡可用取现信用额度', '', ''),
    ('ENCSHMTNOT_ACCENTRAMT', '取现未入账金额', '', ''),
    ('ACCLSTTMENCSHMTAHN_DT', '账户上次取现授权日期', '', ''),
    ('LSTPRDENCSHMT_ADJ_AMT', '上期取现调整金额', '', ''),
    ('LSTPRD_CNSMP_ADJ_AMT', '上期消费调整金额', '', ''),
    ('INTDAYENCSHMTREPY_AMT', '当日取现还款金额', '', ''),
    ('INTDAY_CNSMP_REPY_AMT', '当日消费还款金额', '', ''),
    ('LSTTMCRCALSHDREPYMTDT', '上次信用卡账户最后应还款日期', '', ''),
    ('SRC_CD', '来源码', '', ''),
    ('PREV_PCSG_BAL', '上一处理余额', '', ''),
    ('CRCRDINTFPINNRREPYAMT', '信用卡免息期内还款金额', '', ''),
    ('CRCRD_ACG_STCD', '信用卡账务状态代码', '', ''),
    ('CRCRD_CRDISU_BR_NO', '信用卡发卡分行号', '', ''),
    ('YR_FEE_BILL_DSPL_IND', '年费账单显示标志', '', ''),
    ('GLBL_PY_CRD_IND', '全球支付卡标志', '', ''),
    ('PREVDYCSTRCRDBLCNO_CD', '上一天客户记录封锁码代码', '', ''),
    ('PREVDYCRDRCRDBLCNO_CD', '上一天卡片记录封锁码代码', '', ''),
    ('CASHINSTMNTRCHDAHNAMT', '现金分期未达授权金额', '', ''),
    ('YDCRDCRNPRD_CNSMP_BAL', '益贷卡当期消费余额', '', ''),
    ('NEXT_CL_BAL_SUBACC_SN', '下一分类余额子账户序号', '', ''),
    ('CRCRDCLBAL_SUBACC_NUM', '信用卡分类余额子账户个数', '', ''),
    ('CRCRDAVYCLBLSUBACCNUM', '信用卡活动分类余额子账户个数', '', ''),
    ('CRCRDCLSCLBLSUBACCNUM', '信用卡关闭分类余额子账户个数', '', ''),
    ('CRCRDDELCLBLSUBACCNUM', '信用卡删除分类余额子账户个数', '', ''),
    ('CUR_CRCRD_ACG_ST_EFDT', '当前信用卡账务状态生效日期', '', ''),
    ('PREVCRCRD_ACG_ST_EFDT', '上一信用卡账务状态生效日期', '', ''),
    ('PREV_CRCRD_ACG_STCD', '上一信用卡账务状态代码', '', ''),
    ('CNCLACCT_CLSG_IND', '销户结清标志', '', ''),
    ('CNCLACCT_CLSG_DT', '销户结清日期', '', ''),
    ('BILLAUTOINSTM_PRD_NUM', '账单自动分期期数', '', ''),
    ('TFR_HDCG_DCN_PCTG', '转账手续费折扣比例', '', ''),
    ('ENCSHMT_HDCG_DCN_PCTG', '取现手续费折扣比例', '', ''),
    ('FST_APTRPY_ACC_SRC_ID', '第一约定还款账户来源编号', '', ''),
    ('FST_APTRPY_ACC_ID', '第一约定还款账户编号', '', ''),
    ('FST_APTRPY_ACC_BR_NO', '第一约定还款账户分行号', '', ''),
    ('SND_APTRPY_ACC_SRC_ID', '第二约定还款账户来源编号', '', ''),
    ('SND_APTRPY_ACC_ID', '第二约定还款账户编号', '', ''),
    ('SND_APTRPY_ACC_BR_NO', '第二约定还款账户分行号', '', ''),
    ('APNTACCREPY_PCTG_TPCD', '约定账户还款比例类型代码', '', ''),
    ('OBNKAPTRPY_ACC_SRC_ID', '他行约定还款账户来源编号', '', ''),
    ('OBNK_APTRPY_MTDCD', '他行约定还款方式代码', '', ''),
    ('UNNPY_AHPY_AGRM_NO', '银联授权支付协议号', '', ''),
    ('OFPY_ABNDN_IND', '溢缴款放弃标志', '', ''),
    ('ACC_APRV_INSID', '账户审批机构编号', '', ''),
    ('ORG_INST_RGON_ID_CD', '组织机构地域编号代码', '', ''),
    ('YDCRD_MNT_DT', '益贷卡维护日期', '', ''),
    ('MULTI_TENANCY_ID', '多实体标识', '', ''),
    ('ASPD_ID', '可售产品编号', '', ''),
    ('CRCRDAR_ID', '信用卡合约编号', '', ''),
    ('ACC_SN', '账户序号', '', ''),
    ('ACC_CGY_SN', '账户类别序号', '', ''),
    ('CST_ID', '客户编号', '', ''),
    ('BLDAY', '账单日', '', ''),
    ('UPGRD_ACC_SN', '升等账户序号', '', ''),
    ('UPGRD_ACC_PD_ID', '升等账户产品编号', '', ''),
    ('UPGRD_ACC_AR_ID', '升等账户合约编号', '', ''),
    ('HOLD_CRCRD_BNK_NUM', '持有信用卡银行数', '', ''),
    ('ACC_DONT_INF', '账户捐款信息', '', ''),
    ('JNT_CRD_ACCINF_DSC', '联名卡账户信息描述', '', ''),
    ('ACC_RCRD_BLCNO_CD', '账户记录封锁码代码', '', ''),
    ('ACCRCRD_PREV_BLCNO_CD', '账户记录上一封锁码代码', '', ''),
    ('ACC_STCD', '账户状态代码', '', ''),
    ('CRCRD_BLTP_CD', '信用卡账单类型代码', '', ''),
    ('CRCRD_GEN_BLFRQ', '信用卡生成账单频率', '', ''),
    ('EXMPT_LATEPYMTPNY_IND', '免滞纳金标志', '', ''),
    ('EXMPT_EXCDLMT_FEE_IND', '免超限费标志', '', ''),
    ('EXMPT_HDCG_IND', '免手续费标志', '', ''),
    ('MOD_ST_IND', '修改状态标志', '', ''),
    ('CRCRD_CNCLAFVERF_STCD', '信用卡核销状态代码', '', ''),
    ('SMLBALCNCLAVERFRQSDYS', '小余额核销要求天数', '', ''),
    ('CR_BAL_RET_RQS_DYS', '贷记余额返还要求天数', '', ''),
    ('CRCRDCRDISU_OPNACC_DT', '信用卡发卡开户日期', '', ''),
    ('LAST_ST_CHG_DT', '最后状态变化日期', '', ''),
    ('CRCRD_ACC_CLS_DT', '信用卡账户关闭日期', '', ''),
    ('CRCRD_LSTTM_CNSMP_DT', '信用卡上次消费日期', '', ''),
    ('CRCRDLSTTM_ENCSHMT_DT', '信用卡上次取现日期', '', ''),
    ('CRCRD_LSTTM_ODUE_DT', '信用卡上次逾期日期', '', ''),
    ('CRCRD_HIST_ODUE_DT', '信用卡历史逾期日期', '', ''),
    ('LSTTM_INTAR_CODT', '上次计息截止日期', '', ''),
    ('INTAR_CODT', '计息截止日期', '', ''),
    ('CRCRD_LSTTM_AVY_DT', '信用卡上次活动日期', '', ''),
    ('LSTTMENCSHMTREPYDYPRD', '上次取现还款日期', '', ''),
    ('LSTTMCNSMP_REPYDY_PRD', '上次消费还款日期', '', ''),
    ('LAST_UDT_LMT_DT', '最后更新额度日期', '', ''),
    ('LSTTM_UDT_LMT_DT', '上次更新额度日期', '', ''),
    ('LSTTM_GEN_BLDAY_PRD', '上次生成账单日期', '', ''),
    ('NXT_GEN_BLDAY_PRD', '下次生成账单日期', '', ''),
    ('CRCRDACCLSHLDREPYMTDT', '信用卡账户最后应还款日期', '', ''),
    ('CRCRD_ACC_HGST_BAL_DT', '信用卡账户最高余额日期', '', ''),
    ('CRCRDLSTTM_REPYDY_PRD', '信用卡上次还款日期', '', ''),
    ('BALDRC_MDF_DT', '余额方向变更日期', '', ''),
    ('LSTTM_RET_CHK_DT', '上次退回支票日期', '', ''),
    ('LSTTM_DBT_TXN_DT', '上次借记交易日期', '', ''),
]