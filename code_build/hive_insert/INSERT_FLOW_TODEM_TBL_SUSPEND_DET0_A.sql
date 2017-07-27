--输入输出数据
--IN_CUR_HDFS="/bigdata/input/TS150/case_trace/TODEM_TBL_SUSPEND_DET0_A"
--OUT_CUR_HIVE="sor.INN_TODEM_TBL_SUSPEND_DET0_A"

use sor;

-- Hive贴源数据处理
-- 挂起交易明细表: TODEM_TBL_SUSPEND_DET0_A

-- 指定新数据日期分区位置
ALTER TABLE EXT_TODEM_TBL_SUSPEND_DET0_A DROP IF EXISTS PARTITION(LOAD_DATE='${log_date}');
ALTER TABLE EXT_TODEM_TBL_SUSPEND_DET0_A ADD PARTITION (LOAD_DATE='${log_date}')
            LOCATION 'hdfs://hacluster/bigdata/input/TS150/case_trace/TODEM_TBL_SUSPEND_DET0_A/${log_date}/';

-- 备份贴源数据到ORC内部表
INSERT OVERWRITE TABLE INN_TODEM_TBL_SUSPEND_DET0_A PARTITION(P9_DATA_DATE='${log_date}')
SELECT
    FLOW_NO,                             -- 交易流水号
    CONFIRM_FLOW_NO,                     -- 
    BLACK_ID,                            -- 
    CUST_NO,                             -- CCBS客户编号
    CHANL_NO,                            -- 渠道编号
    CUST_TYPE,                           -- 客户类型
    CHANL_TRAD_NO,                       -- 交易码
    CHANL_ADAP,                          -- 
    ACCT_NO,                             -- 帐号
    TRAD_DATE,                           -- 交易日期
    TRAD_TIME,                           -- 交易时间
    AMT1,                                -- 金额1
    TRAD_BRAN,                           -- 交易机构
    CONFIRM_SERVICE,                     -- 
    OTHER_SERVICE,                       -- 
    RULE_ID,                             -- 规则编号
    RULE_DESC,                           -- 规则描述
    OPT_ID,                              -- 选项编号
    PROC_DATE,                           -- 处理日期
    PROC_TIME,                           -- 
    AP_STATUS,                           -- 
    STATUS,                              -- 状态
    DEAL_FLAG                            -- 优化前后标志
  FROM EXT_TODEM_TBL_SUSPEND_DET0_A
 WHERE LOAD_DATE='${log_date}';
