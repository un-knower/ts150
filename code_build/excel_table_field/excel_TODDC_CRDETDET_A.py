#field_en, field_cn, field_type, field_length, field_is_pk, field_is_dk, field_to_ctbase, index_describe, index_function, index_split, field_filter
table_cn = 'POS交易明细档'
field_array = [
    ('CRDET_LL', '(空值)', '', ''),
    ('CR_POS_REF_NO', 'POS编号', 'Y', 'y'),
    ('CR_TX_DT', '交易日期', 'Y', 'y'),
    ('CR_POS_TX_SQ_NO', 'POS交易序号', '', ''),
    ('CR_EC_FLG', '冲正标志', '', 'y'),
    ('CRDET_DB_TIMESTAMP', '(空值)', '', ''),
    ('CR_RSV_FLG', '备用标志', '', ''),
    ('CR_CURR_COD', '币别', '', ''),
    ('CR_OPR_NO', '操作员号', '', 'y'),
    ('CR_DCC_FLG', '受理DCC标志', '', ''),
    ('CR_DCC_FEE', '商户DCC手续费收入', '', ''),
    ('CR_CHK_FLG', '核对标志', '', ''),
    ('CR_DSC_AMT1', '回扣金额1', '', ''),
    ('CR_DSC_AMT2', '回扣金额2', '', ''),
    ('CR_ENTR_DT', '记帐日期', '', ''),
    ('CR_CPU_DT', '系统日期', '', ''),
    ('CR_GCRD_TX_SQ_NO', '金卡交易流水号', '', ''),
    ('CR_DRGN_CRD_TX_SQ_NO', '龙卡交易流水号', '', ''),
    ('CR_TX_NO', '交易代号', 'Y', 'y'),
    ('CR_TX_AMT', '交易金额', 'Y', 'y'),
    ('CR_ACCT_NO', '帐号', 'Y', 'y'),
    ('CR_TX_LOG_NO', '交易流水号', 'Y', ''),
    ('CR_TX_TM', '交易时间', 'Y', 'y'),
    ('CR_CONF_FLG', '确认标志', '', ''),
    ('CR_RECON_DT', '销帐日期', '', ''),
    ('CR_BRIEF', '摘要说明', 'Y', 'y'),
    ('CR_BATCH_TOTL', '批次', '', ''),
    ('CR_POS_FLG', 'POS轧帐标志', '', ''),
    ('CR_ORDER_NO', '订单号', '', ''),
    ('CR_TIM', '分期消费期数', '', ''),
    ('CR_LNK_TX_SQ_NO', '关联交易序号', '', ''),
    ('CR_LNK_EC_FLG', '关联冲正标志', '', ''),
    ('CR_POS_CUT_DT', 'POS轧帐日期', '', ''),
    ('CR_CRD_ISSU_ORGN_FLG', '发卡组织标识', '', ''),
    ('CR_AUTH_NO', '授权号', '', ''),
    ('CR_TEMP_FLG', '暂扣标志', '', 'y'),
    ('CR_ISSUE_NETN_FLG', '发卡行网络标志', '', ''),
    ('CR_ENTER_BRANCH', '记帐机构', '', ''),
    ('CR_SPARE_AMT1', '回佣分配应付金额', '', ''),
    ('CR_SPARE_AMT2', '备用金额2', '', ''),
    ('CR_SPARE_AMT3', '备用金额3', '', ''),
    ('CR_SPARE_AMT4', '备用金额4', '', ''),
    ('CR_SPARE_AMT5', '备用金额5', '', ''),
    ('CR_RCRD_INSTN_NO', '账号记账机构', 'Y', ''),
    ('CR_FALLBACK_FLAG', '降级交易标志', '', ''),
    ('CR_CNSPN_ACCT_NO', '消费账户', '', ''),
    ('CR_CRD_SEQ_NO', '卡序列号', '', ''),
    ('CR_TREATBACK_SHARE_FLG', '回佣分配帐号标志', '', ''),
    ('CR_FILLER50', 'FILLER5N/A', '', ''),
    ('P9_SPLIT_BRANCH_CD', '按机构拆分字段', '', ''),
    ('P9_DATA_DATE', 'P9数据日期', 'Y', ''),
    ('P9_BATCH_NUMBER', 'P9批次号', '', ''),
    ('P9_DEL_FLAG', '', '', ''),
    ('P9_DEL_DATE', 'P9删除日期', '', ''),
    ('P9_DEL_BATCH', 'P9删除批次号', '', ''),
    ('P9_JOB_NAME', '', '', ''),
]
