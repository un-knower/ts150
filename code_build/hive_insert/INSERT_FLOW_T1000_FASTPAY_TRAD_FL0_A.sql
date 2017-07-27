--输入输出数据
--IN_CUR_HDFS="/bigdata/input/TS150/case_trace/T1000_FASTPAY_TRAD_FL0_A"
--OUT_CUR_HIVE="sor.INN_T1000_FASTPAY_TRAD_FL0_A"

use sor;

-- Hive贴源数据处理
-- 快捷支付交易痕迹: T1000_FASTPAY_TRAD_FL0_A

-- 指定新数据日期分区位置
ALTER TABLE EXT_T1000_FASTPAY_TRAD_FL0_A DROP IF EXISTS PARTITION(LOAD_DATE='${log_date}');
ALTER TABLE EXT_T1000_FASTPAY_TRAD_FL0_A ADD PARTITION (LOAD_DATE='${log_date}')
            LOCATION 'hdfs://hacluster/bigdata/input/TS150/case_trace/T1000_FASTPAY_TRAD_FL0_A/${log_date}/';

-- 备份贴源数据到ORC内部表
INSERT OVERWRITE TABLE INN_T1000_FASTPAY_TRAD_FL0_A PARTITION(P9_DATA_DATE='${log_date}')
SELECT
    MOBILE,                              -- 手机号
    IP                                   -- IP地址
  FROM EXT_T1000_FASTPAY_TRAD_FL0_A
 WHERE LOAD_DATE='${log_date}';
