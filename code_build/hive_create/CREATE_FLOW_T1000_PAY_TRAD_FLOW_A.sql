use sor;

-- Hive建表脚本
-- 交易支付痕迹: T1000_PAY_TRAD_FLOW_A

-- 外部表
DROP TABLE IF EXISTS EXT_T1000_PAY_TRAD_FLOW_A;

CREATE EXTERNAL TABLE IF NOT EXISTS EXT_T1000_PAY_TRAD_FLOW_A(
    PLAT_FLOW_NO              string comment '电子渠道平台流水号',
    CHNL_ID                   string comment '渠道编号',
    CHNL_ADAP_ID              string comment '渠道适配号',
    BSN_CHNL_ID               string comment '业务渠道标识',
    PLAT_TXN_DATE             string comment '平台交易日期',
    PLAT_TXN_TIME             string comment '平台交易时间',
    PLAT_SYS_DATE             string comment '平台系统日期',
    PLAT_TXN_CODE             string comment '电子渠道平台交易码',
    BNK_BSN_ID                string comment '银行业务编号',
    SYS_RESP_CODE             string comment '响应码',
    RSP_INF                   string comment '响应信息',
    TERM_INF                  string comment '终端信息',
    TRN_ST_CD                 string comment '交易状态',
    ORI_TXN_ST                string comment '原交易状态',
    REQ_RCNCL_IND             string comment '需要对账标志',
    RCNCL_RSLT_CD             string comment '对账结果代码',
    CRD_AHN_ID                string comment '卡授权编号',
    CHNL_TXN_CD               string comment '渠道交易码',
    CHAL_FLOW_NO              string comment '渠道流水号',
    CHNL_TXN_DATE             string comment '渠道交易日期',
    BACK_SYS_NM               string comment '后台系统名称',
    BACK_TRAD_DATE            string comment '后台交易日期',
    BACK_FLOW_NO              string comment '后台流水号',
    CR_HJNO                   string comment '贷记主机流水号',
    TXN_TLR_NO                string comment '交易柜员号',
    AHN_TRID                  string comment '授权柜员编号',
    TOT_FLG                   string comment '日交易累计标志',
    PY_LIST_SN                string comment '支付列表序号',
    TXN_RMRK                  string comment '交易备注',
    TRDPT_TXN_DT              string comment '第三方交易日期',
    TRDPT_JRNL_NO             string comment '第三方流水号',
    TRDPT_ACCNO               string comment '第三方账号',
    CNTPR_ACC_TPCD            string comment '交易对手账户类型',
    TRDPT_ACC_DEPBNK_NO       string comment '第三方账户开户行号',
    TRDPT_ACCNM               string comment '第三方户名',
    SETL_ACC                  string comment '结算账户',
    POS_ID                    string comment 'POS编号',
    TXN_MRCH_ID               string comment '交易商户编号',
    CST_NM                    string comment '客户名称',
    SIGN_INSID                string comment '签约机构编号',
    SIGN_ST_TP                string comment '签约状态类型',
    CHNL_CUST_NO              string comment '渠道客户号',
    MRCH_EBNKG_CST_NO         string comment '商户网银客户号',
    CST_NO                    string comment '客户号',
    CST_TPCD                  string comment '客户类型代码',
    KEY_CST_LVL_CD            string comment '重点客户级别代码',
    ENTP_OPR_ID               string comment '企业操作员编号',
    CSTMGR_ID                 string comment '客户经理编号',
    ACC_DPBKINNO              string comment '账户开户行机构号',
    ACCNO_TP                  string comment '账号类型',
    ACC_ACCNM                 string comment '账户户名',
    TXNAMT                    string comment '交易金额',
    SUB_TXNAMT                string comment '子交易金额',
    ORIG_TXNAMT               string comment '原始交易金额',
    APLY_TXNAMT               string comment '申请交易金额',
    HDCG                      string comment '手续费',
    CCYCD                     string comment '币种代码',
    ACCNO                     string comment '账号',
    TXN_INSNO                 string comment '交易机构号',
    PRIM_TXN_IND              string comment '主交易标志',
    MRCH_ID                   string comment '商户编号',
    ORDR_ID                   string comment '订单编号',
    SLDR_INSID                string comment '出售方机构编号',
    CSHEX_CD                  string comment '钞汇代码',
    INSTM_TXN_IND             string comment '分期交易标志',
    INSTM_PRD_NUM             string comment '分期期数',
    RBT_AMT                   string comment '回扣金额',
    CVR_PD_CGYCD              string comment '险种产品类别代码',
    CVR_PD_CGY_NM             string comment '险种产品类别名称',
    LVL2_IDX_ID               string comment '二级指标编号',
    LVL2_IDX_NM               string comment '二级指标名称',
    LVL2_INSID                string comment '二级机构编号',
    LVL2_INST_NM              string comment '二级机构名称',
    PLTFRM_NM                 string comment '平台名称',
    ACTHURL_ADR               string comment '附件URL地址',
    TXN_TPCD                  string comment '交易类型代码',
    TXN_TP_NM                 string comment '交易类型名称',
    RMRK_1                    string comment '备注一',
    RMRK_2                    string comment '备注二',
    FST_RMRK                  string comment '第一备注',
    SND_RMRK                  string comment '第二备注',
    RSRV_FLD_1                string comment '保留字段一',
    RSRV_FLD_2                string comment '保留字段二',
    LOCK_IND                  string comment '锁定标志',
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
DROP TABLE IF EXISTS INN_T1000_PAY_TRAD_FLOW_A;

CREATE TABLE IF NOT EXISTS INN_T1000_PAY_TRAD_FLOW_A(
    MOBILE                    string comment '手机号',
    IP                        string comment 'IP地址'
)
PARTITIONED BY (P9_DATA_DATE string)
STORED AS ORC;
