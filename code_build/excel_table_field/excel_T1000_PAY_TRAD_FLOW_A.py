#field_en, field_cn, field_type, field_length, field_is_pk, field_is_dk, field_to_ctbase, index_describe, index_function, index_split, field_filter
table_cn = '交易支付痕迹'
field_array = [
    ('PLAT_FLOW_NO', '电子渠道平台流水号', '', ''),
    ('CHNL_ID', '渠道编号', '', ''),
    ('CHNL_ADAP_ID', '渠道适配号', '', ''),
    ('BSN_CHNL_ID', '业务渠道标识', '', ''),
    ('PLAT_TXN_DATE', '平台交易日期', '', ''),
    ('PLAT_TXN_TIME', '平台交易时间', '', ''),
    ('PLAT_SYS_DATE', '平台系统日期', '', ''),
    ('PLAT_TXN_CODE', '电子渠道平台交易码', '', ''),
    ('BNK_BSN_ID', '银行业务编号', '', ''),
    ('SYS_RESP_CODE', '响应码', '', ''),
    ('RSP_INF', '响应信息', '', ''),
    ('TERM_INF', '终端信息', '', ''),
    ('TRN_ST_CD', '交易状态', '', ''),
    ('ORI_TXN_ST', '原交易状态', '', ''),
    ('REQ_RCNCL_IND', '需要对账标志', '', ''),
    ('RCNCL_RSLT_CD', '对账结果代码', '', ''),
    ('CRD_AHN_ID', '卡授权编号', '', ''),
    ('CHNL_TXN_CD', '渠道交易码', '', ''),
    ('CHAL_FLOW_NO', '渠道流水号', '', ''),
    ('CHNL_TXN_DATE', '渠道交易日期', '', ''),
    ('BACK_SYS_NM', '后台系统名称', '', ''),
    ('BACK_TRAD_DATE', '后台交易日期', '', ''),
    ('BACK_FLOW_NO', '后台流水号', '', ''),
    ('CR_HJNO', '贷记主机流水号', '', ''),
    ('TXN_TLR_NO', '交易柜员号', '', ''),
    ('AHN_TRID', '授权柜员编号', '', ''),
    ('TOT_FLG', '日交易累计标志', '', ''),
    ('PY_LIST_SN', '支付列表序号', '', ''),
    ('TXN_RMRK', '交易备注', '', ''),
    ('TRDPT_TXN_DT', '第三方交易日期', '', ''),
    ('TRDPT_JRNL_NO', '第三方流水号', '', ''),
    ('TRDPT_ACCNO', '第三方账号', '', ''),
    ('CNTPR_ACC_TPCD', '交易对手账户类型', '', ''),
    ('TRDPT_ACC_DEPBNK_NO', '第三方账户开户行号', '', ''),
    ('TRDPT_ACCNM', '第三方户名', '', ''),
    ('SETL_ACC', '结算账户', '', ''),
    ('POS_ID', 'POS编号', '', ''),
    ('TXN_MRCH_ID', '交易商户编号', '', ''),
    ('CST_NM', '客户名称', '', ''),
    ('SIGN_INSID', '签约机构编号', '', ''),
    ('SIGN_ST_TP', '签约状态类型', '', ''),
    ('CHNL_CUST_NO', '渠道客户号', '', ''),
    ('MRCH_EBNKG_CST_NO', '商户网银客户号', '', ''),
    ('CST_NO', '客户号', '', ''),
    ('CST_TPCD', '客户类型代码', '', ''),
    ('KEY_CST_LVL_CD', '重点客户级别代码', '', ''),
    ('ENTP_OPR_ID', '企业操作员编号', '', ''),
    ('CSTMGR_ID', '客户经理编号', '', ''),
    ('ACC_DPBKINNO', '账户开户行机构号', '', ''),
    ('ACCNO_TP', '账号类型', '', ''),
    ('ACC_ACCNM', '账户户名', '', ''),
    ('TXNAMT', '交易金额', '', ''),
    ('SUB_TXNAMT', '子交易金额', '', ''),
    ('ORIG_TXNAMT', '原始交易金额', '', ''),
    ('APLY_TXNAMT', '申请交易金额', '', ''),
    ('HDCG', '手续费', '', ''),
    ('CCYCD', '币种代码', '', ''),
    ('ACCNO', '账号', '', ''),
    ('TXN_INSNO', '交易机构号', '', ''),
    ('PRIM_TXN_IND', '主交易标志', '', ''),
    ('MRCH_ID', '商户编号', '', ''),
    ('ORDR_ID', '订单编号', '', ''),
    ('SLDR_INSID', '出售方机构编号', '', ''),
    ('CSHEX_CD', '钞汇代码', '', ''),
    ('INSTM_TXN_IND', '分期交易标志', '', ''),
    ('INSTM_PRD_NUM', '分期期数', '', ''),
    ('RBT_AMT', '回扣金额', '', ''),
    ('CVR_PD_CGYCD', '险种产品类别代码', '', ''),
    ('CVR_PD_CGY_NM', '险种产品类别名称', '', ''),
    ('LVL2_IDX_ID', '二级指标编号', '', ''),
    ('LVL2_IDX_NM', '二级指标名称', '', ''),
    ('LVL2_INSID', '二级机构编号', '', ''),
    ('LVL2_INST_NM', '二级机构名称', '', ''),
    ('PLTFRM_NM', '平台名称', '', ''),
    ('ACTHURL_ADR', '附件URL地址', '', ''),
    ('TXN_TPCD', '交易类型代码', '', ''),
    ('TXN_TP_NM', '交易类型名称', '', ''),
    ('RMRK_1', '备注一', '', ''),
    ('RMRK_2', '备注二', '', ''),
    ('FST_RMRK', '第一备注', '', ''),
    ('SND_RMRK', '第二备注', '', ''),
    ('RSRV_FLD_1', '保留字段一', '', ''),
    ('RSRV_FLD_2', '保留字段二', '', ''),
    ('LOCK_IND', '锁定标志', '', ''),
    ('P9_DATA_DATE', 'P9数据日期', '', ''),
    ('P9_BATCH_NUMBER', '42', '', ''),
    ('P9_DEL_FLAG', '', '', ''),
    ('P9_DEL_DATE', '42', '', ''),
    ('P9_DEL_BATCH', '42', '', ''),
    ('P9_JOB_NAME', '', '', ''),
]
