#field_en, field_cn, field_type, field_length, field_is_pk, field_is_dk, field_to_ctbase, index_describe, index_function, index_split, field_filter
table_cn = '个人活期存款明细扩展档'
field_array = [
    ('SAETX_LL', '记录长度', '', ''),
    ('SA_ACCT_NO', '帐号', 'Y', 'y'),
    ('SA_DDP_ACCT_NO_DET_N', '活存帐户明细号', 'Y', 'y'),
    ('SAETX_DB_TIMESTAMP', '时间戳', 'Y', ''),
    ('SA_OP_ACCT_NO_32', '对方帐号', 'Y', 'y'),
    ('SA_OP_CUST_NAME', '对方户名', 'Y', 'y'),
    ('SA_RMRK', '备注', 'Y', 'y'),
    ('SA_OPR_NO', '操作员号', 'Y', 'y'),
    ('SA_SPV_NO', '授权员号', 'Y', 'y'),
    ('SA_OP_BANK_NO', '对方行号', 'Y', 'y'),
    ('SA_B2B_B2C_RMRK', '外系统支付备注', 'Y', 'y'),
    ('FILLER', 'FILLER', '', ''),
    ('P9_SPLIT_BRANCH_CD', '按机构拆分字段', '', ''),
    ('P9_DATA_DATE', 'P9数据日期', 'Y', ''),
    ('P9_BATCH_NUMBER', 'P9批次号', '', ''),
    ('P9_DEL_FLAG', '', '', ''),
    ('P9_DEL_DATE', 'P9删除日期', '', ''),
    ('P9_DEL_BATCH', 'P9删除批次号', '', ''),
    ('P9_JOB_NAME', '', '', ''),
]
