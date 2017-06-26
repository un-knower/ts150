#field_en, field_cn, field_type, field_length, field_is_pk, field_is_dk, field_to_ctbase, index_describe, index_function, index_split, field_filter
table_cn = 'POS主档'
field_array = [
    ('CRPOS_LL', '固定记录长度', '', ''),
    ('CR_POS_REF_NO', 'POS编号', 'Y', ''),
    ('CRPOS_DB_TIMESTAMP', '时间戳', '', ''),
    ('CR_SHOP_NO', '商户编号', 'Y', ''),
    ('CR_OPR_DATE', '操作日期', '', ''),
    ('CR_OPR_NO', '操作员号', '', ''),
    ('CR_COLLC_FCURR_CRD_FLG', '代收外币卡标志', '', ''),
    ('CR_CUNT_NO', '柜台编号', 'Y', ''),
    ('CR_CUNT_TEL_NO', '柜台电话号码', 'Y', ''),
    ('CR_CUNT_TX_CHAR', '柜台交易性质', '', ''),
    ('CR_CUNT_NAME', '柜台名称', 'Y', ''),
    ('CR_CUNT_FLOR', '柜台所在楼层', 'Y', ''),
    ('CR_MACH_TYP', '机型', 'Y', ''),
    ('CR_CONN_TEL_NO', '联系电话', 'Y', ''),
    ('CR_LIN_STS', '线路状况', '', ''),
    ('CR_POS_CUT_TM', 'POS 轧帐时间', '', ''),
    ('CR_POS_CUT_DT', 'POS轧帐日期', '', ''),
    ('CR_AUTH_OTLN_FLG', '允许脱机标志', 'Y', ''),
    ('CR_MSET_ADDR', '装机地址', 'Y', ''),
    ('CR_MSET_DT', '装机日期', 'Y', ''),
    ('CR_FEE_MOD', '回扣收取方式', '', ''),
    ('CR_CLN_NO', '交换号', '', ''),
    ('CR_STLM_TYP', '结算方式', '', ''),
    ('CR_STLM_ACCT_NO_FLG', '结算帐号标志', '', ''),
    ('CR_CLN_FLG', '交换所号', '', ''),
    ('CR_CLN_ACCT_NO', '交换内部帐号', '', ''),
    ('CR_STLM_ACCT_NO', '结算帐号', 'Y', ''),
    ('CR_SQ_AWBK_NO', '结算开户行号', 'Y', ''),
    ('CR_SQ_AWBK_NAME', '结算开户行名', 'Y', ''),
    ('CR_NET_REFUND_FLG', '允许联机退货标志', '', ''),
    ('CR_DIVIDED_PAYMENT_FLG', '是否分期POS', '', ''),
    ('CR_COMMON_POS_FLG', '普通POS标志', '', ''),
    ('CR_BIG_DIV_SHOP_FLG', '大额分期商户标志', '', ''),
    ('CR_ADPT_SHOP_FLG', '积分兑换商户标志', '', ''),
    ('CR_FEE_SHOP_FLG', '缴费商户标志', '', ''),
    ('CR_FINANCE_TRANS_SHOP_FLG', '财务转账标志', '', ''),
    ('CR_PREFER_SHOP_FLG', '特惠标志', '', ''),
    ('CR_AUTH_DRW_FLG', '允许取现标志', '', ''),
    ('CR_SINGLE_TRADE_LIMIT_AMT', '单笔交易限额', '', ''),
    ('CR_DIV_LIMIT_AMT', '分期消费限额', '', ''),
    ('CR_CONNTR_NAME', '联系人名称', 'Y', ''),
    ('CR_BACK_COMPUTE_TYP', '回扣计算方式', '', ''),
    ('CR_BACK_CLN_NO', '回佣交换号', '', ''),
    ('CR_BACK_CLN_FLAG', '回佣交换所号', '', ''),
    ('CR_BACK_CLN_ACCT_NO', '回佣交换内部帐号', '', ''),
    ('CR_FEE_ACCT_NO_FLG', '回佣结算帐号标志', '', ''),
    ('CR_BACK_SQ_AWBK_NO', '回佣结算开户行号', '', ''),
    ('CR_BACK_SQ_AWBK_NAME', '回佣帐户开户行名', '', ''),
    ('CR_BACK_STLM_ACCT_NO', '回佣支付帐号', '', ''),
    ('CR_SETTLE_CYCLE', '资金清算周期', '', ''),
    ('CR_TERM_TYPE', '终端类型', '', ''),
    ('CR_CARD_CAPTURE_CAP', '是否可以吞卡', '', ''),
    ('CR_PIN_CAPTURE_CAP', '输入密码的能力', '', ''),
    ('CR_DATA_INPUT_CAP', '终端数据输入能力', '', ''),
    ('CR_SHOPTYP', '商户类别', '', ''),
    ('CR_FOREIGN_SINGLE_TRANS_LMT', '外卡单笔交易限额', '', ''),
    ('CR_SHOP_NAME_EN', '商户名称EN', '', ''),
    ('CR_OPEN_VISA_FLG', '开通VISA标志', '', ''),
    ('CR_OPEN_MAST_FLG', '开通MASTERCARD标志', '', ''),
    ('CR_OPEN_AE_FLG', '开通AE标志', '', ''),
    ('CR_OPEN_DIN_FLG', '开通DIN标志', '', ''),
    ('CR_OPEN_JCB_FLG', '开通JCB标志', '', ''),
    ('CR_TCC', 'TCC', '', ''),
    ('CR_ALLOW_INPUT_CARDNO_FLG', '允许手输卡号标志', '', ''),
    ('CR_ALLOW_PRE_AUTH_FLG', '允许预授权标志', '', ''),
    ('CR_ALLOW_TIP_FLG', '允许收小费标志', '', ''),
    ('CR_SHOP_ACCT_NAME_62', '商户帐户名称62', 'Y', ''),
    ('CR_BACK_ACCT_NAME_62', '回佣支付帐户名称62', '', ''),
    ('CR_DIV_SHOPTYP', '分期商户类别', '', ''),
    ('CR_FOR_SHOPTYP', '外卡商户类别', '', ''),
    ('CR_DSC_SET_FLG', '回佣扣率设置标志', '', ''),
    ('CR_UNITE_POS_FL', '是否一体机标志', '', ''),
    ('CR_FOREIGN_DAY_TRANS_LMT', '外卡当日限额', '', ''),
    ('CR_CRD_TYP_CHRG_FLG', '按卡种收费标志', '', ''),
    ('CR_ECASH_ACQUIRER_FLG', '允许受理电子现金标志', '', ''),
    ('CR_LOAD_ACQUIRER_FLG', '允许受理圈存标志', '', ''),
    ('CR_LAST_TX_DT', '最近交易日期', '', ''),
    ('CR_TX_ITEM', '交易笔数', '', ''),
    ('CR_FILLER464', 'FILLER464', '', ''),
    ('P9_SPLIT_BRANCH_CD', '按机构拆分字段', '', ''),
    ('P9_START_DATE', 'P9开始日期', 'Y', ''),
    ('P9_START_BATCH', 'P9开始批次号', '', ''),
    ('P9_END_DATE', 'P9结束日期', 'Y', ''),
    ('P9_END_BATCH', 'P9结束批次号', '', ''),
    ('P9_DEL_FLAG', 'P9删除标志', '', ''),
    ('P9_JOB_NAME', 'P9作业名', '', ''),
]
