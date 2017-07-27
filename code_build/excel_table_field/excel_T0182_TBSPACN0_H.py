#field_en, field_cn, field_type, field_length, field_is_pk, field_is_dk, field_to_ctbase, index_describe, index_function, index_split, field_filter
table_cn = '对私活期存款合约'
field_array = [
    ('MULTI_TENANCY_ID', '多实体标识', '', ''),
    ('APL_PRTN_NO', '应用分区号', '', ''),
    ('CST_ACCNO', '客户账号', 'Y', 'Y'),
    ('DEPAR_SN', '存款合约序号', '', ''),
    ('DEPAR_ID', '存款合约编号', 'Y', 'Y'),
    ('IDCST_ACCNO_NM', '个人客户账号名称', 'Y', 'Y'),
    ('ASPD_ASB_VNO', '可售产品装配版本号', '', ''),
    ('ASPD_ID', '可售产品编号', '', ''),
    ('FNDS_USE_CD', '资金用途代码', '', ''),
    ('CST_ID', '客户编号', 'Y', 'Y'),
    ('CSTMGR_USR_ID', '客户经理用户编号', 'Y', 'Y'),
    ('PRVT_DEPAR_STCD', '对私存款合约状态代码', 'Y', 'Y'),
    ('SEAL_CRD_CFINF_ID', '印鉴卡认证信息编号', '', ''),
    ('OPNACC_DT', '开户日期', 'Y', 'Y'),
    ('OPNACC_CHNL_ID', '开户渠道编号', 'Y', 'Y'),
    ('OPNACC_EMPID', '开户员工编号', 'Y', 'Y'),
    ('DPBKINNO', '开户机构编号', 'Y', 'Y'),
    ('LDGR_INSID', '核算机构编号', '', ''),
    ('OPNACC_CHNL_TPCD', '开户渠道类型代码', '', ''),
    ('CNCLACCT_CHNL_TPCD', '销户渠道类型代码', '', ''),
    ('CNCLACCT_DT', '销户日期', 'Y', 'Y'),
    ('CNCLACCT_CHNL_ID', '销户渠道编号', 'Y', 'Y'),
    ('CNCLACCT_EMPID', '销户员工编号', 'Y', 'Y'),
    ('CNCLACCT_INSID', '销户机构编号', 'Y', 'Y'),
    ('RCVPY_CTRL_STCD', '收付控制状态代码', '', ''),
    ('ORI_RCVPY_CTRL_STCD', '原收付控制状态代码', '', ''),
    ('SLP_STCD', '睡眠状态代码', 'Y', 'Y'),
    ('SLP_DT', '睡眠日期', 'Y', 'Y'),
    ('DEP_TFR_SLP_TPCD', '存款转睡眠类型代码', 'Y', 'Y'),
    ('SLP_SWTBCK_APRV_STCD', '睡眠转回审批状态代码', '', ''),
    ('DEP_ALRDY_PRT_DTL_NUM', '存款已打印明细数', '', ''),
    ('DEP_ALRDY_PRT_PG_NUM', '存款已打印页数', '', ''),
    ('ALRDY_PRT_LNUM', '已打印行数', '', ''),
    ('DEP_VERF_MTDCD', '存款校验方式代码', '', ''),
    ('FCS_SIGN_NUM', '关注签约数量', '', ''),
    ('PD_SALE_FTA_CD', '产品销售自贸区代码', '', ''),
    ('DEP_OPNACC_CRDT_TPCD', '存款开户证件类型代码', 'Y', 'Y'),
    ('DEP_BKLTNO', '存款存折册号', '', ''),
    ('PSBK_LOSSRGST_DT', '存折挂失日期', 'Y', 'Y'),
    ('DEP_PSBK_STCD', '存款存折状态代码', 'Y', 'Y'),
    ('PRVTDEPSIGNTPLIST_VAL', '对私存款签约类型列表值', '', ''),
    ('LNCRDREPYACCNOSIGNNUM', '贷记卡还款账号签约数量', '', ''),
    ('IDV_FRNCY_ACCHAR_CD', '个人外币账户性质代码', '', ''),
    ('CST_PINOFFSET_VAL', '客户PINOFFSET值', '', ''),
    ('DEP_PSWD_STCD', '存款密码状态代码', '', ''),
    ('DVV_SAFE_INDX_ID', 'DVV安全索引编号', '', ''),
    ('DEP_ACCHAR_CD', '存款账户性质代码', '', ''),
    ('CSHEX_CHAR_CD', '钞汇性质代码', '', ''),
    ('PSWD_WRG_CNT', '密码出错次数', 'Y', 'Y'),
    ('PSWD_LOSSRGST_DT', '密码挂失日期', 'Y', 'Y'),
    ('SUSP_4_BIT_PSWD_IND', '疑似4位密码标志', '', ''),
    ('DBCRD_AR_ID', '借记卡合约编号', 'Y', 'Y'),
    ('DBCRD_CARDNO', '借记卡卡号', 'Y', 'Y'),
    ('DEP_PSBK_PRT_NO', '存款存折印刷号', '', ''),
    ('UNVSDEP_SCOP_CD', '通存范围代码', '', ''),
    ('UNVSWD_SCOP_CD', '通兑范围代码', '', ''),
    ('CST_ACCNM_VLD_IND', '客户户名验证标志', '', ''),
    ('INTAR_IND', '计息标志', '', ''),
    ('OPNACC_AMT', '开户金额', 'Y', 'Y'),
    ('CUR_DTL_MAX_SN', '当前明细最大序号', '', ''),
    ('CUR_DTL_LAST_TXN_DT', '当前明细最后交易日期', '', ''),
    ('INTDYIDVCACNYENCMTAMT', '当日个人活期账户人民币取现金额', '', ''),
    ('INTDYIDVCAUSDENCMTAMT', '当日个人活期账户美元取现金额', '', ''),
    ('INTDY_INT_DTL_SN', '当日首笔明细序号', '', ''),
    ('DEP_OPNACC_CRDT_NO', '存款开户证件号码', 'Y', 'Y'),
    ('ACC_RSK_GRD_TPCD', '账户风险等级类型代码', '', ''),
    ('SETL_ACC_CLCD', '结算账户分类代码', 'Y', 'Y'),
    ('LAST_UDT_OPRGDAY', '最后更新营业日', '', ''),
    ('TMS', '时间戳', '', ''),
    ('P9_SPLIT_BRANCH_CD', '来源行', '', ''),
    ('P9_START_DATE', 'P9开始日期', 'Y', 'Y'),
    ('P9_START_BATCH', '', '', ''),
    ('P9_END_DATE', 'P9结束日期', 'Y', 'Y'),
    ('P9_END_BATCH', '', '', ''),
    ('P9_DEL_FLAG', '', '', ''),
    ('P9_JOB_NAME', '', '', ''),
]