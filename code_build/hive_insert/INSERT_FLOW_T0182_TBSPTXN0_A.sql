--输入输出数据
--IN_CUR_HDFS="/bigdata/input/TS150/case_trace/T0182_TBSPTXN0_A"
--OUT_CUR_HIVE="sor.INN_T0182_TBSPTXN0_A"

use sor;

-- Hive贴源数据处理
-- 对私活期存款合约明细: T0182_TBSPTXN0_A

-- 指定新数据日期分区位置
ALTER TABLE EXT_T0182_TBSPTXN0_A DROP IF EXISTS PARTITION(LOAD_DATE='${log_date}');
ALTER TABLE EXT_T0182_TBSPTXN0_A ADD PARTITION (LOAD_DATE='${log_date}')
            LOCATION 'hdfs://hacluster/bigdata/input/TS150/case_trace/T0182_TBSPTXN0_A/${log_date}/';

-- 备份贴源数据到ORC内部表
INSERT OVERWRITE TABLE INN_T0182_TBSPTXN0_A PARTITION(P9_DATA_DATE='${log_date}')
SELECT
    CST_ACCNO,                           -- 客户账号
    DEPAR_SN,                            -- 存款合约序号
    regexp_replace(TXN_DT, '-', ''),     -- 交易日期
    ACC_DTL_SN,                          -- 账户明细序号
    CCYCD,                               -- 币种代码
    OVRLSTTN_EV_TRCK_NO,                 -- 全局事件跟踪号
    SMY_CD,                              -- 摘要代码
    TXN_RMRK,                            -- 交易备注
    regexp_replace(TXN_LCL_DT, '-', ''),  -- 交易本地日期
    TXN_LCL_TM,                          -- 交易本地时间
    DEP_DHAMT,                           -- 存款借方发生额
    DEP_CR_HPNAM,                        -- 存款贷方发生额
    DEP_TXNAMT,                          -- 存款交易金额
    DEP_ACBA,                            -- 存款账户余额
    TXN_ITT_CHNL_TPCD,                   -- 交易发起渠道类型代码
    TXN_CHNL_ID,                         -- 交易渠道编号
    TXN_EMPID,                           -- 交易员工编号
    TXN_INSID,                           -- 交易机构编号
    CNTRPRTBOOKENTR_ACCNO,               -- 对方记账账号
    CNTRPRTBOOKENTRACNONM,               -- 对方记账账号名称
    CNTRPRT_KPACCBNK_NO,                 -- 对方记账行号
    CNTRPRT_TXN_ACCNO,                   -- 对方交易账号
    CNTRPRT_TXN_ACCNO_NM,                -- 对方交易账号名称
    CNTRPRT_TXN_PY_BRNO,                 -- 对方交易支付行号
    CNTRPRT_TRDBRH_NM,                   -- 对方交易行名
    CNTRPRT_TXN_MEDM_ID,                 -- 对方交易介质编号
    CNTRPRT_TXN_MEDM_TPCD,               -- 对方交易介质类型代码
    CHNL_TXN_UDF_INF_DSC,                -- 渠道交易自定义信息描述
    EXOSTM_PY_RMRK,                      -- 外系统支付备注
    MRCH_ID,                             -- 商户编号
    AVL_BAL                              -- 可用余额
  FROM EXT_T0182_TBSPTXN0_A
 WHERE LOAD_DATE='${log_date}';
