#field_en, field_cn, field_type, field_length, field_is_pk, field_is_dk, field_to_ctbase, index_describe, index_function, index_split, field_filter
table_cn = 'ATM控制档'
field_array = [
    ('CRATM_LL', '(空值)', '', ''),
    ('CR_ATM_NO', 'ATM编号', 'Y', ''),
    ('CRATM_DB_TIMESTAMP', '(空值)', '', ''),
    ('CR_OPUN_COD', '营业单位代码', 'Y', ''),
    ('CR_ATM_MACH_TYP', 'ATM机型', 'Y', ''),
    ('CR_ATM_TYP', 'ATM型号', 'Y', ''),
    ('CR_OPR_DATE', '操作日期', '', ''),
    ('CR_OPR_NO', '操作员号', '', ''),
    ('CR_CASH_OPR_STS', '出钞状态', '', ''),
    ('CR_DEP_BOX_STS', '存款箱状态', '', ''),
    ('CR_PRT_STS', '打印机状态', '', ''),
    ('CR_OFL_TM', '断线时间', '', ''),
    ('CR_ATM_STS', '开关机状态', '', ''),
    ('CR_WHLD_CRD_PCS', '留置卡片张数', '', ''),
    ('CR_ITEM_TOTL', '提款总笔数', '', ''),
    ('CR_DRW_AMT_TOTL', '取款总金额', '', ''),
    ('CR_CHDT_ITEM_TOTL', '现金存款总笔数', '', ''),
    ('CR_CHDT_AMT_TOTL', '现金存款总金额', '', ''),
    ('CR_LIN_STS', '线路状况', '', ''),
    ('CR_ENVL_QTY', '信封数量', '', ''),
    ('CR_WK_DATE', '营业日期', '', ''),
    ('CR_PPRL', '纸卷', '', ''),
    ('CR_LAST_ITEM_TOTL', '上次提款总笔数', '', ''),
    ('CR_LAST_DRW_AMT_TOTL', '上次取款总金额', '', ''),
    ('CR_LAST_CHDT_ITEM_TOTL', '上次现金存款总笔数', '', ''),
    ('CR_LAST_CHDT_AMT_TOTL', '上次现金存款总金额', '', ''),
    ('CR_LAST_CHKOUT_DT', '上次轧帐日期', '', ''),
    ('CR_LLAST_CHKOUT_DT', '上上次轧帐日期', '', ''),
    ('CR_LLAST_CHKOUT_ATM_SQ_NO', '上上次轧帐ATM交易序号', '', ''),
    ('CR_LAST_CHKOUT_ATM_SQ_NO', '上次轧帐ATM交易序号', '', ''),
    ('CR_FILLER60', 'FILLER6N/A', '', ''),
    ('CR_MSET_ADDR', '装机地址', 'Y', ''),
    ('CR_MSET_DT', '装机日期', 'Y', ''),
    ('CR_ATM_TX_SQ_NO', 'ATM交易序号', '', ''),
    ('CR_NO_CLN_AMT', '未轧帐金额', '', ''),
    ('CR_CLN_AMT', '已轧帐金额', '', ''),
    ('FILLER', 'FILLER', '', ''),
    ('P9_SPLIT_BRANCH_CD', '按机构拆分字段', '', ''),
    ('P9_START_DATE', 'P9开始日期', 'Y', ''),
    ('P9_START_BATCH', 'P9开始批次号', '', ''),
    ('P9_END_DATE', 'P9结束日期', 'Y', ''),
    ('P9_END_BATCH', 'P9结束批次号', '', ''),
    ('P9_DEL_FLAG', 'P9删除标志', '', ''),
    ('P9_JOB_NAME', 'P9作业名', '', ''),
]
