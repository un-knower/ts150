use sor;

-- Hive建表脚本
-- 对私活期存款合约明细: T0182_TBSPTXN0_A

-- 外部表
DROP TABLE IF EXISTS EXT_T0182_TBSPTXN0_A;

CREATE EXTERNAL TABLE IF NOT EXISTS EXT_T0182_TBSPTXN0_A(
    MULTI_TENANCY_ID          string comment '多实体标识',
    APL_PRTN_NO               string comment '应用分区号',
    CST_ACCNO                 string comment '客户账号',
    DEPAR_SN                  string comment '存款合约序号',
    TXN_DT                    string comment '交易日期',
    ACC_DTL_SN                string comment '账户明细序号',
    CCYCD                     string comment '币种代码',
    CSHEX_CD                  string comment '钞汇代码',
    OVRLSTTN_EV_TRCK_NO       string comment '全局事件跟踪号',
    SYS_SND_SERIAL_NO         string comment '子交易序号',
    TRD_TP_ECD                string comment '交易种类编码',
    CMPT_TRCNO                string comment '组件流水号',
    DEP_TXN_CGYCD             string comment '存款交易类别代码',
    SMY_CD                    string comment '摘要代码',
    DEP_TXN_MEDM_ID           string comment '存款交易介质编号',
    TXN_MEDM_TPCD             string comment '交易介质类型代码',
    BILL_CTCD                 string comment '票据种类代码',
    BILL_NO                   string comment '票据号码',
    VALDT                     string comment '起息日期',
    TXN_RMRK                  string comment '交易备注',
    TXN_LCL_DT                string comment '交易本地日期',
    TXN_LCL_TM                string comment '交易本地时间',
    STM_DT                    string comment '系统日期',
    STM_TM                    string comment '系统时间',
    DEP_DHAMT                 string comment '存款借方发生额',
    DEP_CR_HPNAM              string comment '存款贷方发生额',
    DEP_TXNAMT                string comment '存款交易金额',
    BSN_TXN_ITT_MTDCD         string comment '业务交易发起方式代码',
    DEP_ACBA                  string comment '存款账户余额',
    DEP_ACM                   string comment '存款积数',
    TXN_ITT_CHNL_TPCD         string comment '交易发起渠道类型代码',
    TXN_CHNL_ID               string comment '交易渠道编号',
    TXN_EMPID                 string comment '交易员工编号',
    TXN_INSID                 string comment '交易机构编号',
    FST_AHN_EMPID             string comment '第一授权员工编号',
    SND_AHN_EMPID             string comment '第二授权员工编号',
    CSTMGR_USR_ID             string comment '客户经理用户编号',
    CNTRPRTBOOKENTR_ACCNO     string comment '对方记账账号',
    CNTRPRTBOOKENTRACNONM     string comment '对方记账账号名称',
    CNTRPRT_KPACCBNK_NO       string comment '对方记账行号',
    CNTRPRT_TXN_ACCNO         string comment '对方交易账号',
    CNTRPRT_TXN_ACCNO_NM      string comment '对方交易账号名称',
    CNTRPRT_TXN_PY_BRNO       string comment '对方交易支付行号',
    CNTRPRT_TRDBRH_NM         string comment '对方交易行名',
    CNTRPRT_TXN_MEDM_ID       string comment '对方交易介质编号',
    CNTRPRT_TXN_MEDM_TPCD     string comment '对方交易介质类型代码',
    CHNL_TXN_UDF_INF_DSC      string comment '渠道交易自定义信息描述',
    RVRS_CD                   string comment '冲正代码',
    RVRS_DT                   string comment '冲正日期',
    RVRS_ACC_DTL_SN           string comment '冲正账户明细序号',
    EVT_TRACE_ID_EC           string comment '原交易全局事件跟踪号',
    SND_SERIAL_NO_EC          string comment '原交易子交易序号',
    ORI_TXN_CTLG_ECD          string comment '原交易种类编码',
    ORI_CMPT_TRCNO            string comment '原组件流水号',
    SYS_TX_CODE               string comment '交易服务编码',
    EXOSTM_PY_RMRK            string comment '外系统支付备注',
    MRCH_ID                   string comment '商户编号',
    INTAR_PNP_AMT             string comment '计息本金额',
    INT_AMT                   string comment '利息金额',
    DEP_BOOKENTRAM            string comment '存款记账金额',
    AVL_BAL                   string comment '可用余额',
    OD_INT_PNAMT              string comment '计透支息本金金额',
    BNK_CRD_OD_ACM            string comment '银行卡透支积数',
    BNK_CRD_OD_INT_AMT        string comment '银行卡透支利息金额',
    DEP_LGL_FRZ_AMT           string comment '存款司法冻结金额',
    LGL_ACT_FRZ_AMT           string comment '司法实际冻结金额',
    ITRT_BSN_FRZ_AMT          string comment '在途业务冻结金额',
    ORD_BSN_FRZ_AMT           string comment '普通业务冻结金额',
    AGNC_PSN_IND              string comment '代理人标志',
    DTL_APD_INF_IND           string comment '明细附加信息标志',
    ACT_DT                    string comment '会计日期',
    DBCRD_AHN_ID              string comment '借记卡授权编号',
    CCBS_STM_TXN_CD           string comment 'CCBS系统交易码',
    PAIDINTAXBNKCCOINTAMT     string comment '已缴税银行卡催收透支利息金额',
    TPYTAXBNKCRDCODINTAMT     string comment '待缴税银行卡催收透支利息金额',
    LAST_UDT_OPRGDAY          string comment '最后更新营业日',
    TMS                       string comment '时间戳',
    P9_SPLIT_BRANCH_CD        string comment '来源行',
    P9_DATA_DATE              string comment 'P9数据日期',
    P9_BATCH_NUMBER           string comment '',
    P9_DEL_FLAG               string comment '',
    P9_DEL_DATE               string comment '',
    P9_DEL_BATCH              string comment '',
    P9_JOB_NAME               string comment ''
)
PARTITIONED BY (LOAD_DATE string)
STORED AS TEXTFILE;

-- ORC内部表，节约存储空间
DROP TABLE IF EXISTS INN_T0182_TBSPTXN0_A;

CREATE TABLE IF NOT EXISTS INN_T0182_TBSPTXN0_A(
    CST_ACCNO                 string comment '客户账号',
    DEPAR_SN                  string comment '存款合约序号',
    TXN_DT                    string comment '交易日期',
    ACC_DTL_SN                string comment '账户明细序号',
    CCYCD                     string comment '币种代码',
    OVRLSTTN_EV_TRCK_NO       string comment '全局事件跟踪号',
    SMY_CD                    string comment '摘要代码',
    TXN_RMRK                  string comment '交易备注',
    TXN_LCL_DT                string comment '交易本地日期',
    TXN_LCL_TM                string comment '交易本地时间',
    DEP_DHAMT                 string comment '存款借方发生额',
    DEP_CR_HPNAM              string comment '存款贷方发生额',
    DEP_TXNAMT                string comment '存款交易金额',
    DEP_ACBA                  string comment '存款账户余额',
    TXN_ITT_CHNL_TPCD         string comment '交易发起渠道类型代码',
    TXN_CHNL_ID               string comment '交易渠道编号',
    TXN_EMPID                 string comment '交易员工编号',
    TXN_INSID                 string comment '交易机构编号',
    CNTRPRTBOOKENTR_ACCNO     string comment '对方记账账号',
    CNTRPRTBOOKENTRACNONM     string comment '对方记账账号名称',
    CNTRPRT_KPACCBNK_NO       string comment '对方记账行号',
    CNTRPRT_TXN_ACCNO         string comment '对方交易账号',
    CNTRPRT_TXN_ACCNO_NM      string comment '对方交易账号名称',
    CNTRPRT_TXN_PY_BRNO       string comment '对方交易支付行号',
    CNTRPRT_TRDBRH_NM         string comment '对方交易行名',
    CNTRPRT_TXN_MEDM_ID       string comment '对方交易介质编号',
    CNTRPRT_TXN_MEDM_TPCD     string comment '对方交易介质类型代码',
    CHNL_TXN_UDF_INF_DSC      string comment '渠道交易自定义信息描述',
    EXOSTM_PY_RMRK            string comment '外系统支付备注',
    MRCH_ID                   string comment '商户编号',
    AVL_BAL                   string comment '可用余额'
)
PARTITIONED BY (P9_DATA_DATE string)
STORED AS ORC;