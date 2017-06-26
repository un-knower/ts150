#field_en, field_cn, field_type, field_length, field_is_pk, field_is_dk, field_to_ctbase, index_describe, index_function, index_split, field_filter
table_cn = '信用卡账单争议明细'
field_array = [
    ('ACC_SN', '账户序号', '', ''),
    ('ACMD_DT', '账户变更日期', '', ''),
    ('ACPT_MRCH_NM', '受理商户名称', 'Y', ''),
    ('ACPT_MRCH_NO', '受理商户号', 'Y', ''),
    ('AHN_TXNAMT', '授权交易金额', '', ''),
    ('ASPD_ID', '可售产品编号', '', ''),
    ('BILL_REF_SEQ_NO', '账单参考序列号', '', ''),
    ('CCBINS_ID', '建行机构编号', 'Y', ''),
    ('CRCRD_ACG_SETL_DT', '信用卡账务结算日期', '', ''),
    ('CRCRD_CARDNO', '信用卡卡号', 'Y', ''),
    ('CRCRD_CRD_CGY_SN', '信用卡卡片类别序号', '', ''),
    ('CRCRD_INR_TXN_CD', '信用卡内部交易码', 'Y', ''),
    ('CRCRD_INSTMTXN_CD', '信用卡分期交易代码', '', ''),
    ('CRCRD_LAST_UDT_EMPID', '信用卡最后更新员工编号', '', ''),
    ('CRCRD_MNASBCRD_CD', '信用卡主附卡代码', 'Y', ''),
    ('CRCRD_TXN_ACCENTR_AMT', '信用卡交易入账金额', 'Y', ''),
    ('CRCRDAR_ID', '信用卡合约编号', 'Y', ''),
    ('CRCRDTXN_OPNT_ACCNO', '信用卡交易对手账号', 'Y', ''),
    ('CRCRDTXN_OPNT_BKNM', '信用卡交易对手行名', 'Y', ''),
    ('CRCRDTXN_OPNT_BRNO', '信用卡交易对手行号', 'Y', ''),
    ('CRCRDTXN_OPNT_NM', '信用卡交易对手名称', 'Y', ''),
    ('CRD_AHN_CD', '卡授权码', '', ''),
    ('CRD_AHN_INF_TNCCY_CD', '卡授权信息交易币种代码', '', ''),
    ('CRD_AHN_TXN_DT', '卡授权交易日期', 'Y', ''),
    ('CRD_AHN_TXN_TM', '卡授权交易时间', 'Y', ''),
    ('CST_ID', '客户编号', 'Y', ''),
    ('DAC_VERF', 'DAC校验', '', ''),
    ('DCC_AMT', 'DCC金额', 'Y', ''),
    ('DCC_CCYCD', 'DCC币种代码', '', ''),
    ('DPAN_CARDNO', 'DPAN卡号', '', ''),
    ('DSPL_FLD_IND', '显示字段标志', '', ''),
    ('ED_CRD_PRTY_NM_ADR', '受卡方名称地址', 'Y', ''),
    ('EDCRDMCHN_TMNL_IDR_CD', '受卡机终端标识码', 'Y', ''),
    ('FILE_SEQ_NO', '文件序列号', '', ''),
    ('HGACC_IND', '挂账标志', '', ''),
    ('IDV_CORPCRD_IDCD', '个人公司卡标识代码', '', ''),
    ('LAST_MOD_TM', '最后修改时间', '', ''),
    ('MRCH_CGY_CD', '商户类别码', '', ''),
    ('MRCH_CTY_ECD', '商户国家编码', '', ''),
    ('MULTI_TENANCY_ID', '多实体标识', '', ''),
    ('OPRGDAY_PRD', '营业日期', '', ''),
    ('ORI_TNCCY_CD', '原交易币种代码', '', ''),
    ('ORIG_ACPT_INST_IDR_CD', '原始受理机构标识码', '', ''),
    ('ORIG_CLRG_CRNY_CD', '原始清算币别代码', '', ''),
    ('ORIG_CLRG_DT', '原始清算日期', '', ''),
    ('ORIG_CLRGAMT', '原始清算金额', '', ''),
    ('ORIG_TXN_DT', '原交易日期', '', ''),
    ('ORIG_TXN_TM', '原始交易时间', '', ''),
    ('ORIG_TXNAMT', '原始交易金额', '', ''),
    ('ORIGBILL_REF_ID', '原账单参考编号', '', ''),
    ('ORIOVRLSTTNEV_TRCK_NO', '原全局事件跟踪号', '', ''),
    ('RMTANDTFR_FUD_ID', '汇划款项编号', '', ''),
    ('RSRV_FLD_DSC', '预留字段描述', '', ''),
    ('SRC_CD', '来源码', '', ''),
    ('SUB_MRCH_NM', '子商户名称', 'Y', ''),
    ('SUB_MRCH_NO', '子商户号', 'Y', ''),
    ('SYS_EVT_TRACE_ID', '全局事件跟踪号(流水号)', 'Y', ''),
    ('TMS', '时间戳', '', ''),
    ('TXN_ITT_CHNL_CGY_CODE', '交易发起渠道类别', '', ''),
    ('TXN_ITT_CHNL_ID', '交易发起渠道编号', '', ''),
    ('TXN_NM_DSC', '交易名称描述', 'Y', ''),
    ('TXN_TP_NM', '交易类型名称', 'Y', ''),
    ('VNO', '版本号', '', ''),
    ('P9_SPLIT_BRANCH_CD', '按机构拆分字段', '', ''),
    ('P9_START_DATE', 'P9开始日期', 'Y', ''),
    ('P9_START_BATCH', 'P9开始批次号', '', ''),
    ('P9_END_DATE', 'P9结束日期', 'Y', ''),
    ('P9_END_BATCH', 'P9结束批次号', '', ''),
    ('P9_DEL_FLAG', 'P9删除标志', '', ''),
    ('P9_JOB_NAME', 'P9作业名', '', ''),
]
