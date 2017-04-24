#!/bin/sh

#引用基础Shell函数库
source /home/ap/dip_ts150/ts150_script/ccb_risk_scoring/base.sh

#解释命令行参数
logdate_arg $*

#新集群登陆
hadoop_login

db=train

IN_CUR_HIVE="train_feature_card_90_day"
OUT_CUR_HIVE="train_feature_crdt_90_day"

#设置日期（当天，前7天，前30天，前90天）
p9_data_date=$log_date

less_7_date=`date -d "$p9_data_date 7 days ago" +"%Y%m%d"`
less_30_date=`date -d "$p9_data_date 30 days ago" +"%Y%m%d"`
less_90_date=`date -d "$p9_data_date 90 days ago" +"%Y%m%d"`

echo "cur = ${p9_data_date}"
echo "7 = ${less_7_date}"
echo "30=${less_30_date}"
echo "90=${less_90_date}"


#插入pass 统计信息
export_feature_crdt_90(){

beeline <<!
use $db;

insert overwrite table train_feature_crdt_90_day partition (p9_data_date = '$p9_data_date')
select
    crdt_no,
    sum(all_cur_day_trade_num),
    sum(all_cur_day_trade_amt),
    sum(all_cur_day_in_num),
    sum(all_cur_day_in_amt),
    sum(all_cur_day_out_num),
    sum(all_cur_day_out_amt),
    sum(all_cur_day_trn_out_num),
    sum(all_cur_day_trn_out_amt),
    sum(all_cur_day_500_out_num),
    sum(all_cur_day_500_out_amt),
    sum(all_cur_day_10000_out_num),
    sum(all_cur_day_10000_out_amt),
    sum(all_cur_day_20000_out_num),
    sum(all_cur_day_20000_out_amt),
    sum(all_cur_day_more_out_num),
    sum(all_cur_day_more_out_amt),
    sum(atm_cur_day_dep_num),
    sum(atm_cur_day_dep_amt),
    sum(atm_cur_day_wd_num),
    sum(atm_cur_day_wd_amt),
    sum(atm_cur_day_trn_num),
    sum(atm_cur_day_trn_amt),
    sum(pos_cur_day_cnsmp_num),
    sum(pos_cur_day_cnsmp_amt),
    sum(nb_cur_day_out_num),
    sum(nb_cur_day_out_amt),
    sum(mb_cur_day_out_num),
    sum(mb_cur_day_out_amt),
    sum(all_7_day_trade_num),
    sum(all_7_day_trade_amt),
    sum(all_7_day_in_num),
    sum(all_7_day_in_amt),
    sum(all_7_day_out_num),
    sum(all_7_day_out_amt),
    sum(all_7_day_trn_out_num),
    sum(all_7_day_trn_out_amt),
    sum(all_7_day_500_out_num),
    sum(all_7_day_500_out_amt),
    sum(all_7_day_10000_out_num),
    sum(all_7_day_10000_out_amt),
    sum(all_7_day_20000_out_num),
    sum(all_7_day_20000_out_amt),
    sum(all_7_day_more_out_num),
    sum(all_7_day_more_out_amt),
    sum(atm_7_day_dep_num),
    sum(atm_7_day_dep_amt),
    sum(atm_7_day_wd_num),
    sum(atm_7_day_wd_amt),
    sum(atm_7_day_trn_num),
    sum(atm_7_day_trn_amt),
    sum(pos_7_day_cnsmp_num),
    sum(pos_7_day_cnsmp_amt),
    sum(nb_7_day_out_num),
    sum(nb_7_day_out_amt),
    sum(mb_7_day_out_num),
    sum(mb_7_day_out_amt),
    sum(all_30_day_trade_num),
    sum(all_30_day_trade_amt),
    sum(all_30_day_in_num),
    sum(all_30_day_in_amt),
    sum(all_30_day_out_num),
    sum(all_30_day_out_amt),
    sum(all_30_day_trn_out_num),
    sum(all_30_day_trn_out_amt),
    sum(all_30_day_500_out_num),
    sum(all_30_day_500_out_amt),
    sum(all_30_day_10000_out_num),
    sum(all_30_day_10000_out_amt),
    sum(all_30_day_20000_out_num),
    sum(all_30_day_20000_out_amt),
    sum(all_30_day_more_out_num),
    sum(all_30_day_more_out_amt),
    sum(atm_30_day_dep_num),
    sum(atm_30_day_dep_amt),
    sum(atm_30_day_wd_num),
    sum(atm_30_day_wd_amt),
    sum(atm_30_day_trn_num),
    sum(atm_30_day_trn_amt),
    sum(pos_30_day_cnsmp_num),
    sum(pos_30_day_cnsmp_amt),
    sum(nb_30_day_out_num),
    sum(nb_30_day_out_amt),
    sum(mb_30_day_out_num),
    sum(mb_30_day_out_amt),
    sum(all_90_day_trade_num),
    sum(all_90_day_trade_amt),
    sum(all_90_day_in_num),
    sum(all_90_day_in_amt),
    sum(all_90_day_out_num),
    sum(all_90_day_out_amt),
    sum(all_90_day_trn_out_num),
    sum(all_90_day_trn_out_amt),
    sum(all_90_day_500_out_num),
    sum(all_90_day_500_out_amt),
    sum(all_90_day_10000_out_num),
    sum(all_90_day_10000_out_amt),
    sum(all_90_day_20000_out_num),
    sum(all_90_day_20000_out_amt),
    sum(all_90_day_more_out_num),
    sum(all_90_day_more_out_amt),
    sum(atm_90_day_dep_num),
    sum(atm_90_day_dep_amt),
    sum(atm_90_day_wd_num),
    sum(atm_90_day_wd_amt),
    sum(atm_90_day_trn_num),
    sum(atm_90_day_trn_amt),
    sum(pos_90_day_cnsmp_num),
    sum(pos_90_day_cnsmp_amt),
    sum(nb_90_day_out_num),
    sum(nb_90_day_out_amt),
    sum(mb_90_day_out_num),
    sum(mb_90_day_out_amt)

from train_feature_card_90_day 
    where p9_data_date = '$p9_data_date'
 group by crdt_no;

!
}

run()
{
   export_feature_crdt_90
}
