use sor;

-- Hive建表脚本
-- 快捷支付交易痕迹: T1000_FASTPAY_TRAD_FL0_A

-- 外部表
DROP TABLE IF EXISTS EXT_T1000_FASTPAY_TRAD_FL0_A;

CREATE EXTERNAL TABLE IF NOT EXISTS EXT_T1000_FASTPAY_TRAD_FL0_A(
    PLAT_FLOW_NO              string comment '电子渠道平台流水号',
    CHNL_ID                   string comment '渠道编号',
    PLAT_TXN_DATE             string comment '平台交易日期',
    PLAT_TXN_TIME             string comment '平台交易时间',
    PLAT_SYS_DATE             string comment '平台系统日期',
    CHNL_TXN_DATE             string comment '渠道交易日期',
    CHAL_FLOW_NO              string comment '渠道流水号',
    CHNL_TXN_CD               string comment '渠道交易码',
    BACK_SYS_NM               string comment '后台系统名称',
    BACK_TRAD_DATE            string comment '后台交易日期',
    BACK_FLOW_NO              string comment '后台流水号',
    PLAT_TXN_CODE             string comment '电子渠道平台交易码',
    BNK_BSN_ID                string comment '银行业务编号',
    CST_NM                    string comment '客户名称',
    TXNAMT                    string comment '交易金额',
    ORIG_TXNAMT               string comment '原始交易金额',
    APLY_TXNAMT               string comment '申请交易金额',
    HDCG                      string comment '手续费',
    CCYCD                     string comment '币种代码',
    ACCNO                     string comment '账号',
    TRDR_ACC_TPCD             string comment '交易方账户类型代码',
    CNTPR_ACCNO               string comment '交易对手账号',
    CNTPR_ACC_TPCD            string comment '交易对手账户类型',
    TXN_INSID                 string comment '交易机构编号',
    ACC_DPBKINNO              string comment '账户开户行机构号',
    CNTPR_ACC_DPBKINNO        string comment '交易对手账户开户行机构号',
    RTRN_CODE                 string comment '返回码',
    RET_INF                   string comment '返回信息',
    TERM_INF                  string comment '终端信息',
    TRN_ST_CD                 string comment '交易状态',
    SIGN_TPCD                 string comment '签约类型代码',
    PRIM_TXN_IND              string comment '主交易标志',
    INSTM_TXN_IND             string comment '分期交易标志',
    SETL_ACC                  string comment '结算账户',
    MRCH_ID                   string comment '商户编号',
    ORDR_ID                   string comment '订单编号',
    SLDR_INSID                string comment '出售方机构编号',
    POS_ID                    string comment 'POS编号',
    CMDTY_LRGCLSS_CD          string comment '商品大类代码',
    CMDTY_ECD                 string comment '商品编码',
    FST_RMRK                  string comment '第一备注',
    SND_RMRK                  string comment '第二备注',
    CSHEX_CD                  string comment '钞汇代码',
    REQ_RCNCL_IND             string comment '需要对账标志',
    RCNCL_RSLT_CD             string comment '对账结果代码',
    TXN_MRCH_ID               string comment '交易商户编号',
    CMDTY_NM                  string comment '商品名称',
    TXN_RMRK                  string comment '交易备注',
    IDX_SN                    string comment '指标序号',
    CRD_AHN_ID                string comment '卡授权编号',
    PY_LIST_SN                string comment '支付列表序号',
    DFFT_CND_SN               string comment '差异化条件序号',
    LVL2_IDX_ID               string comment '二级指标编号',
    LVL2_IDX_NM               string comment '二级指标名称',
    RLTVTXN_LV1_BSN_CTCD      string comment '关联交易一级业务种类代码',
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
DROP TABLE IF EXISTS INN_T1000_FASTPAY_TRAD_FL0_A;

CREATE TABLE IF NOT EXISTS INN_T1000_FASTPAY_TRAD_FL0_A(
    MOBILE                    string comment '手机号',
    IP                        string comment 'IP地址'
)
PARTITIONED BY (P9_DATA_DATE string)
STORED AS ORC;
