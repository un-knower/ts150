#field_en, field_cn, field_type, field_length, field_is_pk, field_is_dk, field_to_ctbase, index_describe, index_function, index_split, field_filter
table_cn = '客户联系位置信息'
field_array = [
    ('MULTI_TENANCY_ID', '多实体标识', '', ''),
    ('CST_ID', '客户编号', 'Y', ''),
    ('RCRD_EXPY_TMS', '记录失效时间戳', '', ''),
    ('CTC_INF_TPCD', '联系信息类型代码', 'Y', ''),
    ('IDV_CTC_INF_SN', '个人联系信息序号', '', ''),
    ('RETPCD', '关系类型代码', '', ''),
    ('TELCTCMOD_ITNL_DSTCNO', '电话联系方式国际区号', '', ''),
    ('TELCTCMOD_DMST_DSTCNO', '电话联系方式国内区号', '', ''),
    ('TELCTCMOD_NO', '电话联系方式号码', 'Y', ''),
    ('TELCTCMOD_EXN_NO', '电话联系方式分机号码', 'Y', ''),
    ('ZIPECD', '邮政编码', '', ''),
    ('CTYRGON_CD', '国家地区代码', '', ''),
    ('PROV_ATNMSRGON_CD', '省自治区代码', '', ''),
    ('URBN_CD', '城市代码', '', ''),
    ('CNTYANDDSTC_CD', '区县代码', '', ''),
    ('DTL_ADR_CNTNT', '详细地址内容', 'Y', ''),
    ('REL_STDT', '关系开始日期', '', ''),
    ('REL_EDDT', '关系结束日期', '', ''),
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
    ('P9_START_DATE', 'P9开始日期', 'Y', ''),
    ('P9_START_BATCH', '', '', ''),
    ('P9_END_DATE', 'P9结束日期', 'Y', ''),
    ('P9_END_BATCH', '', '', ''),
    ('P9_DEL_FLAG', '', '', ''),
    ('P9_JOB_NAME', '', '', ''),
]
