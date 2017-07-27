--输入输出数据
--IN_CUR_HDFS="/bigdata/input/TS150/case_trace/T0291_BTH_ONL_TXN_JNL_A"
--OUT_CUR_HIVE="sor.INN_T0291_BTH_ONL_TXN_JNL_A"

use sor;

-- Hive贴源数据处理
-- 线上批量交易流水表: T0291_BTH_ONL_TXN_JNL_A

-- 指定新数据日期分区位置
ALTER TABLE EXT_T0291_BTH_ONL_TXN_JNL_A DROP IF EXISTS PARTITION(LOAD_DATE='${log_date}');
ALTER TABLE EXT_T0291_BTH_ONL_TXN_JNL_A ADD PARTITION (LOAD_DATE='${log_date}')
            LOCATION 'hdfs://hacluster/bigdata/input/TS150/case_trace/T0291_BTH_ONL_TXN_JNL_A/${log_date}/';

-- 备份贴源数据到ORC内部表
INSERT OVERWRITE TABLE INN_T0291_BTH_ONL_TXN_JNL_A PARTITION(P9_DATA_DATE='${log_date}')
SELECT
    regexp_replace(POS_OFSTBAL_DT, '-', ''),  -- POS轧账日期
    EBNK_VCHR_NO,                        -- 网银凭证号
    USR_ID_NO,                           -- 用户识别号
    TXN_CD_CHN_NM,                       -- 交易代码中文名称
    CARDNO,                              -- 卡号
    CCYCD,                               -- 币种代码
    TXNAMT,                              -- 交易金额
    TXN_SND_TM,                          -- 交易发送时间
    POS_ID,                              -- POS编号
    SYS_RESP_CODE,                       -- 响应码
    SYS_RESP_DESC,                       -- 服务响应描述
    SYS_EVT_TRACE_ID,                    -- 全局事件跟踪号(流水号)
    CCBINS_ID,                           -- 建行机构编号
    SYS_TX_CODE,                         -- 交易服务编码
    TXN_ITT_CHNL_CGY_CODE,               -- 交易发起渠道类别
    TXN_ITT_CHNL_ID,                     -- 交易发起渠道编号
    EMRCH_MRCH_TPCD,                     -- 电商商户类型代码
    VISA_LV2_MRCH_NO,                    -- VISA二级商户号
    LV2_MRCH_NM,                         -- 二级商户名称
    CNTPR_ACCNO,                         -- 交易对手账号
    CNTPR_ACCNM                          -- 交易对手账户名称
  FROM EXT_T0291_BTH_ONL_TXN_JNL_A
 WHERE LOAD_DATE='${log_date}';