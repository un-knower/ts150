#!/bin/sh

#引用基础Shell函数库
source /home/ap/dip_ts150/ts150_script/ccb_risk_scoring/base.sh

#解释命令行参数
logdate_arg $*

#新集群登陆
hadoop_login

db=train

IN_CUR_HIVE="train_feature_cur_day"
IN_PRE_HIVE="train_feature_card_90_day"
OUT_CUR_HIVE="train_feature_card_90_day"

#设置日期（当天，前7天，前30天，前90天）
p9_data_date=$log_date

less_7_date=`date -d "$p9_data_date 7 days ago" +"%Y%m%d"`
less_30_date=`date -d "$p9_data_date 30 days ago" +"%Y%m%d"`
less_90_date=`date -d "$p9_data_date 90 days ago" +"%Y%m%d"`

echo "cur = ${p9_data_date}"
echo "7 = ${less_7_date}"
echo "30=${less_30_date}"
echo "90=${less_90_date}"


#插入卡统计信息
export_feature_card_90(){

beeline <<!
use $db;

insert overwrite table train_feature_card_90_day partition (p9_data_date = '$p9_data_date')
select
    a.card_no, a.crdt_no,
    a.all_cur_day_trade_num,
    a.all_cur_day_trade_amt,
    a.all_cur_day_in_num,
    a.all_cur_day_in_amt,
    a.all_cur_day_out_num,
    a.all_cur_day_out_amt,
    a.all_cur_day_trn_out_num,
    a.all_cur_day_trn_out_amt,
    a.all_cur_day_500_out_num,
    a.all_cur_day_500_out_amt,
    a.all_cur_day_10000_out_num,
    a.all_cur_day_10000_out_amt,
    a.all_cur_day_20000_out_num,
    a.all_cur_day_20000_out_amt,
    a.all_cur_day_more_out_num,
    a.all_cur_day_more_out_amt,
    a.atm_cur_day_dep_num,
    a.atm_cur_day_dep_amt,
    a.atm_cur_day_wd_num,
    a.atm_cur_day_wd_amt,
    a.atm_cur_day_trn_num,
    a.atm_cur_day_trn_amt,
    a.pos_cur_day_cnsmp_num,
    a.pos_cur_day_cnsmp_amt,
    a.nb_cur_day_out_num,
    a.nb_cur_day_out_amt,
    a.mb_cur_day_out_num,
    a.mb_cur_day_out_amt,
    
    b.all_7_day_trade_num,
    b.all_7_day_trade_amt,
    b.all_7_day_in_num,
    b.all_7_day_in_amt,
    b.all_7_day_out_num,
    b.all_7_day_out_amt,
    b.all_7_day_trn_out_num,
    b.all_7_day_trn_out_amt,
    b.all_7_day_500_out_num,
    b.all_7_day_500_out_amt,
    b.all_7_day_10000_out_num,
    b.all_7_day_10000_out_amt,
    b.all_7_day_20000_out_num,
    b.all_7_day_20000_out_amt,
    b.all_7_day_more_out_num,
    b.all_7_day_more_out_amt,
    b.atm_7_day_dep_num,
    b.atm_7_day_dep_amt,
    b.atm_7_day_wd_num,
    b.atm_7_day_wd_amt,
    b.atm_7_day_trn_num,
    b.atm_7_day_trn_amt,
    b.pos_7_day_cnsmp_num,
    b.pos_7_day_cnsmp_amt,
    b.nb_7_day_out_num,
    b.nb_7_day_out_amt,
    b.mb_7_day_out_num,
    b.mb_7_day_out_amt,
    
    c.all_30_day_trade_num,
    c.all_30_day_trade_amt,
    c.all_30_day_in_num,
    c.all_30_day_in_amt,
    c.all_30_day_out_num,
    c.all_30_day_out_amt,
    c.all_30_day_trn_out_num,
    c.all_30_day_trn_out_amt,
    c.all_30_day_500_out_num,
    c.all_30_day_500_out_amt,
    c.all_30_day_10000_out_num,
    c.all_30_day_10000_out_amt,
    c.all_30_day_20000_out_num,
    c.all_30_day_20000_out_amt,
    c.all_30_day_more_out_num,
    c.all_30_day_more_out_amt,
    c.atm_30_day_dep_num,
    c.atm_30_day_dep_amt,
    c.atm_30_day_wd_num,
    c.atm_30_day_wd_amt,
    c.atm_30_day_trn_num,
    c.atm_30_day_trn_amt,
    c.pos_30_day_cnsmp_num,
    c.pos_30_day_cnsmp_amt,
    c.nb_30_day_out_num,
    c.nb_30_day_out_amt,
    c.mb_30_day_out_num,
    c.mb_30_day_out_amt,
    
    d.all_90_day_trade_num,
    d.all_90_day_trade_amt,
    d.all_90_day_in_num,
    d.all_90_day_in_amt,
    d.all_90_day_out_num,
    d.all_90_day_out_amt,
    d.all_90_day_trn_out_num,
    d.all_90_day_trn_out_amt,
    d.all_90_day_500_out_num,
    d.all_90_day_500_out_amt,
    d.all_90_day_10000_out_num,
    d.all_90_day_10000_out_amt,
    d.all_90_day_20000_out_num,
    d.all_90_day_20000_out_amt,
    d.all_90_day_more_out_num,
    d.all_90_day_more_out_amt,
    d.atm_90_day_dep_num,
    d.atm_90_day_dep_amt,
    d.atm_90_day_wd_num,
    d.atm_90_day_wd_amt,
    d.atm_90_day_trn_num,
    d.atm_90_day_trn_amt,
    d.pos_90_day_cnsmp_num,
    d.pos_90_day_cnsmp_amt,
    d.nb_90_day_out_num,
    d.nb_90_day_out_amt,
    d.mb_90_day_out_num,
    d.mb_90_day_out_amt

from (
   select * 
     from train_feature_cur_day 
    where p9_data_date = '$p9_data_date'
   ) a
inner join (
   SELECT card_no,
        sum(NVL(all_cur_day_trade_num,0)) all_7_day_trade_num,
        sum(NVL(all_cur_day_trade_amt,0)) all_7_day_trade_amt,
        sum(NVL(all_cur_day_in_num,0)) all_7_day_in_num,
        sum(NVL(all_cur_day_in_amt,0)) all_7_day_in_amt,
        sum(NVL(all_cur_day_out_num,0)) all_7_day_out_num,
        sum(NVL(all_cur_day_out_amt,0)) all_7_day_out_amt,
        sum(NVL(all_cur_day_trn_out_num,0)) all_7_day_trn_out_num,
        sum(NVL(all_cur_day_trn_out_amt,0)) all_7_day_trn_out_amt,
        sum(NVL(all_cur_day_500_out_num,0)) all_7_day_500_out_num,
        sum(NVL(all_cur_day_500_out_amt,0)) all_7_day_500_out_amt,
        sum(NVL(all_cur_day_10000_out_num,0)) all_7_day_10000_out_num,
        sum(NVL(all_cur_day_10000_out_amt,0)) all_7_day_10000_out_amt,
        sum(NVL(all_cur_day_20000_out_num,0)) all_7_day_20000_out_num,
        sum(NVL(all_cur_day_20000_out_amt,0)) all_7_day_20000_out_amt,
        sum(NVL(all_cur_day_more_out_num,0)) all_7_day_more_out_num,
        sum(NVL(all_cur_day_more_out_amt,0)) all_7_day_more_out_amt,
        sum(NVL(atm_cur_day_dep_num,0)) atm_7_day_dep_num,
        sum(NVL(atm_cur_day_dep_amt,0)) atm_7_day_dep_amt,
        sum(NVL(atm_cur_day_wd_num,0)) atm_7_day_wd_num,
        sum(NVL(atm_cur_day_wd_amt,0)) atm_7_day_wd_amt,
        sum(NVL(atm_cur_day_trn_num,0)) atm_7_day_trn_num,
        sum(NVL(atm_cur_day_trn_amt,0)) atm_7_day_trn_amt,
        sum(NVL(pos_cur_day_cnsmp_num,0)) pos_7_day_cnsmp_num,
        sum(NVL(pos_cur_day_cnsmp_amt,0)) pos_7_day_cnsmp_amt,
        sum(NVL(nb_cur_day_out_num,0)) nb_7_day_out_num,
        sum(NVL(nb_cur_day_out_amt,0)) nb_7_day_out_amt,
        sum(NVL(mb_cur_day_out_num,0)) mb_7_day_out_num,
        sum(NVL(mb_cur_day_out_amt,0)) mb_7_day_out_amt
     from train_feature_cur_day 
    where p9_data_date > '$less_7_date'
      and p9_data_date <= '$p9_data_date'
    group by card_no
   ) b 
   on a.card_no = b.card_no

inner join (
   SELECT card_no,
        sum(NVL(all_cur_day_trade_num,0)) all_30_day_trade_num,
        sum(NVL(all_cur_day_trade_amt,0)) all_30_day_trade_amt,
        sum(NVL(all_cur_day_in_num,0)) all_30_day_in_num,
        sum(NVL(all_cur_day_in_amt,0)) all_30_day_in_amt,
        sum(NVL(all_cur_day_out_num,0)) all_30_day_out_num,
        sum(NVL(all_cur_day_out_amt,0)) all_30_day_out_amt,
        sum(NVL(all_cur_day_trn_out_num,0)) all_30_day_trn_out_num,
        sum(NVL(all_cur_day_trn_out_amt,0)) all_30_day_trn_out_amt,
        sum(NVL(all_cur_day_500_out_num,0)) all_30_day_500_out_num,
        sum(NVL(all_cur_day_500_out_amt,0)) all_30_day_500_out_amt,
        sum(NVL(all_cur_day_10000_out_num,0)) all_30_day_10000_out_num,
        sum(NVL(all_cur_day_10000_out_amt,0)) all_30_day_10000_out_amt,
        sum(NVL(all_cur_day_20000_out_num,0)) all_30_day_20000_out_num,
        sum(NVL(all_cur_day_20000_out_amt,0)) all_30_day_20000_out_amt,
        sum(NVL(all_cur_day_more_out_num,0)) all_30_day_more_out_num,
        sum(NVL(all_cur_day_more_out_amt,0)) all_30_day_more_out_amt,
        sum(NVL(atm_cur_day_dep_num,0)) atm_30_day_dep_num,
        sum(NVL(atm_cur_day_dep_amt,0)) atm_30_day_dep_amt,
        sum(NVL(atm_cur_day_wd_num,0)) atm_30_day_wd_num,
        sum(NVL(atm_cur_day_wd_amt,0)) atm_30_day_wd_amt,
        sum(NVL(atm_cur_day_trn_num,0)) atm_30_day_trn_num,
        sum(NVL(atm_cur_day_trn_amt,0)) atm_30_day_trn_amt,
        sum(NVL(pos_cur_day_cnsmp_num,0)) pos_30_day_cnsmp_num,
        sum(NVL(pos_cur_day_cnsmp_amt,0)) pos_30_day_cnsmp_amt,
        sum(NVL(nb_cur_day_out_num,0)) nb_30_day_out_num,
        sum(NVL(nb_cur_day_out_amt,0)) nb_30_day_out_amt,
        sum(NVL(mb_cur_day_out_num,0)) mb_30_day_out_num,
        sum(NVL(mb_cur_day_out_amt,0)) mb_30_day_out_amt
     from train_feature_cur_day 
    where p9_data_date > '$less_30_date'
      and p9_data_date <= '$p9_data_date'
    group by card_no
   ) c 
   on a.card_no = c.card_no

inner join (
    SELECT card_no,
        sum(NVL(all_cur_day_trade_num,0)) all_90_day_trade_num,
        sum(NVL(all_cur_day_trade_amt,0)) all_90_day_trade_amt,
        sum(NVL(all_cur_day_in_num,0)) all_90_day_in_num,
        sum(NVL(all_cur_day_in_amt,0)) all_90_day_in_amt,
        sum(NVL(all_cur_day_out_num,0)) all_90_day_out_num,
        sum(NVL(all_cur_day_out_amt,0)) all_90_day_out_amt,
        sum(NVL(all_cur_day_trn_out_num,0)) all_90_day_trn_out_num,
        sum(NVL(all_cur_day_trn_out_amt,0)) all_90_day_trn_out_amt,
        sum(NVL(all_cur_day_500_out_num,0)) all_90_day_500_out_num,
        sum(NVL(all_cur_day_500_out_amt,0)) all_90_day_500_out_amt,
        sum(NVL(all_cur_day_10000_out_num,0)) all_90_day_10000_out_num,
        sum(NVL(all_cur_day_10000_out_amt,0)) all_90_day_10000_out_amt,
        sum(NVL(all_cur_day_20000_out_num,0)) all_90_day_20000_out_num,
        sum(NVL(all_cur_day_20000_out_amt,0)) all_90_day_20000_out_amt,
        sum(NVL(all_cur_day_more_out_num,0)) all_90_day_more_out_num,
        sum(NVL(all_cur_day_more_out_amt,0)) all_90_day_more_out_amt,
        sum(NVL(atm_cur_day_dep_num,0)) atm_90_day_dep_num,
        sum(NVL(atm_cur_day_dep_amt,0)) atm_90_day_dep_amt,
        sum(NVL(atm_cur_day_wd_num,0)) atm_90_day_wd_num,
        sum(NVL(atm_cur_day_wd_amt,0)) atm_90_day_wd_amt,
        sum(NVL(atm_cur_day_trn_num,0)) atm_90_day_trn_num,
        sum(NVL(atm_cur_day_trn_amt,0)) atm_90_day_trn_amt,
        sum(NVL(pos_cur_day_cnsmp_num,0)) pos_90_day_cnsmp_num,
        sum(NVL(pos_cur_day_cnsmp_amt,0)) pos_90_day_cnsmp_amt,
        sum(NVL(nb_cur_day_out_num,0)) nb_90_day_out_num,
        sum(NVL(nb_cur_day_out_amt,0)) nb_90_day_out_amt,
        sum(NVL(mb_cur_day_out_num,0)) mb_90_day_out_num,
        sum(NVL(mb_cur_day_out_amt,0)) mb_90_day_out_amt
     from train_feature_cur_day 
    where p9_data_date > '$less_90_date'
      and p9_data_date <= '$p9_data_date'
    group by card_no
   ) d 
   on a.card_no = d.card_no;
!
}


run()
{
   export_feature_card_90

}
