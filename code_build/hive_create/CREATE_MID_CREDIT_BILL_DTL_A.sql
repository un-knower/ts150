use sor;

-- Hive建表脚本--中间表
-- 信用卡账单表: MID_CREDIT_BILL_DTL_A

-- 中间表
DROP TABLE IF EXISTS MID_CREDIT_BILL_DTL_A;

CREATE TABLE IF NOT EXISTS MID_CREDIT_BILL_DTL_A (
    ACPT_MRCH_NM               string,  -- 受理商户名称
    ACPT_MRCH_NO               string,  -- 受理商户号
    CCBINS_ID                  string,  -- 建行机构编号
    CRCRD_CARDNO               string,  -- 信用卡卡号
    CRCRD_INR_TXN_CD           string,  -- 信用卡内部交易码
    CRCRD_MNASBCRD_CD          string,  -- 信用卡主附卡代码
    CRCRD_TXN_ACCENTR_AMT      string,  -- 信用卡交易入账金额
    CRCRDAR_ID                 string,  -- 信用卡合约编号
    CRCRDTXN_OPNT_ACCNO        string,  -- 信用卡交易对手账号
    CRCRDTXN_OPNT_BKNM         string,  -- 信用卡交易对手行名
    CRCRDTXN_OPNT_BRNO         string,  -- 信用卡交易对手行号
    CRCRDTXN_OPNT_NM           string,  -- 信用卡交易对手名称
    CRD_AHN_TXN_DT             string,  -- 卡授权交易日期
    CRD_AHN_TXN_TM             string,  -- 卡授权交易时间
    CST_ID                     string,  -- 客户编号
    DCC_AMT                    string,  -- DCC金额
    ED_CRD_PRTY_NM_ADR         string,  -- 受卡方名称地址
    EDCRDMCHN_TMNL_IDR_CD      string,  -- 受卡机终端标识码
    SUB_MRCH_NM                string,  -- 子商户名称
    SUB_MRCH_NO                string,  -- 子商户号
    SYS_EVT_TRACE_ID           string,  -- 全局事件跟踪号(流水号)
    TXN_NM_DSC                 string,  -- 交易名称描述
    TXN_TP_NM                  string,  -- 交易类型名称
    CRCRD_CRDISU_BR_NO         string,  -- 信用卡发卡分行号
    CRCRD_HSTCRD_CST_ID        string,  -- 信用卡主卡客户编号
    CRCRD_AFCRD_CST_ID         string,  -- 信用卡附属卡客户编号
    CRD_OPNCRD_DT              string,  -- 卡片开卡日期
    CRD_CLS_DT                 string,  -- 卡片关闭日期
    CRD_GEN_DT                 string,  -- 卡片生成日期
    ACC_1_PD_ID                string,  -- 账户一产品编号
    CRDISU_FST_CRCRDAR_ID      string,  -- 发卡第一信用卡合约编号
    ACC_2_PD_ID                string,  -- 账户二产品编号
    CRDISU_SND_CRCRDAR_ID      string,  -- 发卡第二信用卡合约编号
    ACC_3_PD_ID                string,  -- 账户三产品编号
    CRDISU_TRD_CRCRDAR_ID      string,  -- 发卡第三信用卡合约编号
    CRD_USE_ST_IND             string,  -- 卡使用状态标志
    CRCRD_CMPN_EMPID           string,  -- 信用卡营销员工编号
    CRCRD_CMPN_INSID           string   -- 信用卡营销机构编号
)
PARTITIONED BY (P9_DATA_DATE string)
STORED AS ORC;
