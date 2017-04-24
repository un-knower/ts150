#!/bin/sh

#引用基础Shell函数库
source /home/ap/dip_ts150/ts150_script/ccb_risk_scoring/base.sh

#解释命令行参数
logdate_arg $*

#新集群登陆
hadoop_login

db=train

IN_CUR_HIVE="stat_tx_last_time_a"
#IN_PRE_HIVE=""
OUT_CUR_HIVE="stat_card_tx_last_time_h stat_pass_tx_last_time_h"


first_use_a_to_h()
{
beeline << ! 

     use $db;

     
     insert overwrite table stat_card_tx_last_time_h partition(p9_data_date='${log_date}')
         select CARD_NO,
                concat_ws('|;|',collect_set(concat(flag_type,'|,|',flag_id,'|:|',LAST_DATE,'|:|',LAST_TIME))) as flag
           from stat_tx_last_time_a m
          where p9_data_date='${log_date}'
          group by m.CARD_NO;


 
     insert overwrite table stat_pass_tx_last_time_h partition(p9_data_date='${log_date}')
         select PASS_NUM,
                concat_ws('|;|',collect_set(concat(flag_type,'|,|',flag_id,'|:|',LAST_DATE,'|:|',LAST_TIME))) as flag
         from stat_tx_last_time_a m
         where p9_data_date='${log_date}'
         group by m.PASS_NUM;
!
}

###调用
run()
{
  first_use_a_to_h
}
