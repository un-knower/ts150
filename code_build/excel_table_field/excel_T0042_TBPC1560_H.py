#field_en, field_cn, field_type, field_length, field_is_pk, field_is_dk, field_to_ctbase, index_describe, index_function, index_split, field_filter
table_cn = '客户与合约关系信息'
field_array = [
    ('MULTI_TENANCY_ID', '多实体标识', '', ''),
    ('CST_ID', '客户编号', '', 'n'),
    ('RCRD_EXPY_TMS', '记录失效时间戳', '', ''),
    ('BSPD_ECD', '基础产品编码', '', ''),
    ('RETPCD', '关系类型代码', '', ''),
    ('SRCSYS_AR_ID', '源系统合约编号', '', ''),
    ('RSRV_CST_ID', '备用客户编号', '', ''),
    ('AR_ID', '合约编号', '', ''),
    ('ASPD_ECD', '可售产品编码', '', ''),
    ('SRCSYS_AR_TPDS', '源系统合约类型描述', '', ''),
    ('PD_SIGN_IND', '产品签约标志', '', ''),
    ('INR_AR_IND', '内部合约标志', '', ''),
    ('CST_EXT_ACCNO', '客户外部账号', '', 'n'),
    ('AR_STCD', '合约状态代码', '', ''),
    ('AR_EFDT', '合约生效日期', '', ''),
    ('AR_EXDAT', '合约到期日期', '', ''),
    ('AR_TMDT', '合约终止日期', '', ''),
    ('CRT_INSID', '创建机构编号', '', ''),
    ('CRT_EMPID', '创建员工编号', '', ''),
    ('LAST_UDT_INSID', '最后更新机构编号', '', ''),
    ('LAST_UDT_EMPID', '最后更新员工编号', '', ''),
    ('CUR_STM_CRT_TMS', '当前系统创建时间戳', '', ''),
    ('CUR_STM_UDT_TMS', '当前系统更新时间戳', '', ''),
    ('LCL_YRMO_DAY', '本地年月日', '', ''),
    ('LCL_HR_GRD_SCND', '本地时分秒', '', ''),
    ('CRT_STM_NO', '创建系统号', '', ''),
    ('UDT_STM_NO', '更新系统号', '', ''),
    ('SRCSYS_CRT_TMS', '源系统创建时间戳', '', ''),
    ('SRCSYS_UDT_TMS', '源系统更新时间戳', '', ''),
    ('P9_START_DATE', 'P9开始日期', '', ''),
    ('P9_START_BATCH', '', '', ''),
    ('P9_END_DATE', 'P9结束日期', '', ''),
    ('P9_END_BATCH', '', '', ''),
    ('P9_DEL_FLAG', '', '', ''),
    ('P9_JOB_NAME', '', '', ''),
]
