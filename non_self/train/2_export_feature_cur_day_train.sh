#!/bin/sh

#引用基础Shell函数库
source /home/ap/dip_ts150/ts150_script/ccb_risk_scoring/base.sh

#解释命令行参数
logdate_arg $*

#新集群登陆
hadoop_login

db=train

IN_CUR_HIVE="main_trade_flow_ccbs_atm_pos_ectip_card_user_a"
OUT_CUR_HIVE="train_feature_cur_day"

#设置日期（当天，前7天，前30天，前90天）
p9_data_date=$log_date


#插入p9_data_date当天卡统计信息
export_feature_cur(){

beeline <<!
use $db;

insert overwrite table train_feature_cur_day  partition (p9_data_date = '$p9_data_date')
select SA_TX_CRD_NO, CRDT_NO,
count(*),
sum(SA_TX_AMT),
sum(case when SA_CR_AMT > 0 then 1 else 0 end),
sum(case when SA_CR_AMT > 0 then SA_CR_AMT else 0 end),
sum(case when SA_DR_AMT > 0 then 1 else 0 end),
sum(case when SA_DR_AMT > 0 then SA_DR_AMT else 0 end),
sum(case when SA_OP_ACCT_NO_32 is not null then 1 else 0 end),
sum(case when SA_OP_ACCT_NO_32 is not null then SA_DR_AMT else 0 end),
sum(case when SA_DR_AMT > 0 and SA_DR_AMT <= 500 then 1 else 0 end),
sum(case when SA_DR_AMT > 0 and SA_DR_AMT <= 500 then SA_DR_AMT else 0 end),
sum(case when SA_DR_AMT > 500 and SA_DR_AMT <= 10000 then 1 else 0 end),
sum(case when SA_DR_AMT > 500 and SA_DR_AMT <= 10000 then SA_DR_AMT else 0 end),
sum(case when SA_DR_AMT > 10000 and SA_DR_AMT <= 20000 then 1 else 0 end),
sum(case when SA_DR_AMT > 10000 and SA_DR_AMT <= 20000 then SA_DR_AMT else 0 end),
sum(case when SA_DR_AMT > 20000 then 1 else 0 end),
sum(case when SA_DR_AMT > 20000 then SA_DR_AMT else 0 end),

sum(case when(SA_CR_AMT > 0 and SA_OP_ACCT_NO_32 is null and CR_ATM_NO is not null) then 1 else 0 end),
sum(case when(SA_CR_AMT > 0 and SA_OP_ACCT_NO_32 is null and CR_ATM_NO is not null) then SA_CR_AMT else 0 end),
sum(case when(SA_DR_AMT > 0 and SA_OP_ACCT_NO_32 is null and CR_ATM_NO is not null) then 1 else 0 end),
sum(case when(SA_DR_AMT > 0 and SA_OP_ACCT_NO_32 is null and CR_ATM_NO is not null) then SA_DR_AMT else 0 end),
sum(case when(SA_DR_AMT > 0 and SA_OP_ACCT_NO_32 is not null and CR_ATM_NO is not null) then 1 else 0 end),
sum(case when(SA_DR_AMT > 0 and SA_OP_ACCT_NO_32 is not null and CR_ATM_NO is not null) then SA_DR_AMT else 0 end),
               
sum(case when CR_POS_REF_NO is not null then 1 else 0 end),
sum(case when CR_POS_REF_NO is not null then CR_TX_AMT_POS else 0 end),

sum(case when (CHANL_NO='01' or CHANL_NO='0003') then 1 else 0 end),
sum(case when (CHANL_NO='01' or CHANL_NO='0003') then AMT1 else 0 end),

sum(case when CHANL_NO='02' then 1 else 0 end),
sum(case when CHANL_NO='02' then AMT1 else 0 end)
       
from ${db}.main_trade_flow_ccbs_atm_pos_ectip_card_user_a
where P9_DATA_DATE = '$p9_data_date'
group by SA_TX_CRD_NO, crdt_no;

!
}

run()
{
   export_feature_cur
}

#date1='20151106'
#date2='20161206'
#
#for ((i=$date1; i<=$date2;i=`date -d "$i 1 days" +"%Y%m%d"`))do
#   echo "======================", $i, "================================="
#   p9_data_date=$i
#   export_feature_cur
#done

