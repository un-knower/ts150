#!/bin/sh

#引用基础Shell函数库
source /home/ap/dip_ts150/ts150_script/ccb_risk_scoring/base.sh

#解释命令行参数
logdate_arg $*

#新集群登陆
hadoop_login

db=train

IN_CUR_HIVE="main_trade_flow_ccbs_atm_pos_ectip_card_user_a"
IN_PRE_HIVE="stat_tx_last_time_a"
OUT_CUR_HIVE="stat_tx_last_time_a"


fill_today()
{

beeline << !

set role admin;

add jar hdfs://hacluster/bigdata/common/function/siam-etl-p2log-inputformat.jar;
add jar hdfs://hacluster/bigdata/common/function/siam-etl-p2log-hive_udf.jar;
add jar hdfs://hacluster/bigdata/common/function/dom4j-1.6.1.jar;

-- 取非空字符串
create function ts150.ts150_GetNotEmpty as 'com.ccb.ts150.p2log.GetNotEmpty_UDF' using jar 'hdfs://hacluster/bigdata/common/function/siam-etl-p2log-hive_udf.jar';


set hive.exec.parallel=true;

use $db;

FROM (
   select sa_tx_crd_no as card_no, CRDT_NO as pass_num, 
          SA_TX_DT as tx_date, SA_TX_TM as tx_time,
          ts150.ts150_GetNotEmpty(CR_ATM_NO, CR_POS_REF_NO, mobile, term_qry) as device,
          SA_APP_TX_CODE, SA_OP_ACCT_NO_32, SA_OPUN_COD, ip, SA_DR_AMT
      from main_trade_flow_ccbs_atm_pos_ectip_card_user_a 
     where p9_data_date='${log_date}'
       and sa_tx_crd_no <> ''
       and CRDT_NO <> ''
  ) t
insert overwrite table stat_tx_last_time_a partition(p9_data_date='${log_date}', flag_type='1')
  select t.card_no, t.pass_num, t.device, max(tx_date), max(tx_time)
   where t.device is not null
     and t.device <> ''
   group by t.card_no, t.pass_num, t.device

insert overwrite table stat_tx_last_time_a partition(p9_data_date='${log_date}', flag_type='2')
  select t.card_no, t.pass_num, t.SA_APP_TX_CODE, max(tx_date), max(tx_time)
   where t.SA_APP_TX_CODE is not null
     and t.SA_APP_TX_CODE <> ''
   group by t.card_no, t.pass_num, t.SA_APP_TX_CODE

insert overwrite table stat_tx_last_time_a partition(p9_data_date='${log_date}', flag_type='3')
  select t.card_no, t.pass_num, t.SA_OP_ACCT_NO_32, max(tx_date), max(tx_time)
   where t.SA_OP_ACCT_NO_32 is not null
     and t.SA_OP_ACCT_NO_32 <> ''
   group by t.card_no, t.pass_num, t.SA_OP_ACCT_NO_32

insert overwrite table stat_tx_last_time_a partition(p9_data_date='${log_date}', flag_type='4')
  select t.card_no, t.pass_num, t.SA_OPUN_COD, max(tx_date), max(tx_time)
   where t.SA_OPUN_COD is not null
     and t.SA_OPUN_COD <> ''
   group by t.card_no, t.pass_num, t.SA_OPUN_COD

insert overwrite table stat_tx_last_time_a partition(p9_data_date='${log_date}', flag_type='5')
  select t.card_no, t.pass_num, t.ip, max(tx_date), max(tx_time)
   where t.ip is not null
     and t.ip <> ''
   group by t.card_no, t.pass_num, t.ip

insert overwrite table stat_tx_last_time_a partition(p9_data_date='${log_date}', flag_type='6')
  select t.card_no, t.pass_num, '6', max(tx_date), max(tx_time)
   group by t.card_no, t.pass_num

insert overwrite table stat_tx_last_time_a partition(p9_data_date='${log_date}', flag_type='7')
  select t.card_no, t.pass_num, '7', max(tx_date), max(tx_time)
   where float(t.SA_DR_AMT) > 0
   group by t.card_no, t.pass_num
!
}

fill_yesterday()
{
beeline << ! 

set role admin;

add jar hdfs://hacluster/bigdata/common/function/ts150_hive_tools.jar;

create function ts150.my_date_add as 'com.ccb.ts150.hive_udf.MyDateAdd_UDF' using jar 'hdfs://hacluster/bigdata/common/function/ts150_hive_tools.jar';

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;

set hive.exec.parallel=true;
use $db;

insert into table stat_tx_last_time_a partition(p9_data_date='${log_date}', flag_type)
 select t1.card_no, t1.pass_num, t1.flag_id, t1.last_date, t1.last_time, t1.flag_type
   from (
    select card_no, pass_num, flag_id, last_date, last_time, flag_type
      from stat_tx_last_time_a 
     where p9_data_date=ts150.my_date_add('${log_date}', -1)
       and (
           ( flag_type = '1' and
             last_date >= ts150.my_date_add('${log_date}', -90))
        or ( flag_type = '2' and
             last_date >= ts150.my_date_add('${log_date}', -180))
        or ( flag_type = '3' and 
             last_date >= ts150.my_date_add('${log_date}', -180))
        or ( flag_type = '4' and 
             last_date >= ts150.my_date_add('${log_date}', -90))
        or ( flag_type = '5' and 
             last_date >= ts150.my_date_add('${log_date}', -90))
        or ( flag_type = '6' and 
             last_date >= ts150.my_date_add('${log_date}', -90))
        or ( flag_type = '7' and 
             last_date >= ts150.my_date_add('${log_date}', -90))
       )
    )t1
   left outer join (
        select card_no, pass_num, flag_id, last_date, last_time, flag_type
          from stat_tx_last_time_a
        where p9_data_date = '${log_date}'
          and flag_id <> ''
    )t2
     on t1.card_no = t2.card_no
    and t1.flag_id = t2.flag_id
    and t1.flag_type = t2.flag_type
  where t2.card_no is null
  distribute by flag_type;

insert into table stat_tx_last_time_a partition(p9_data_date='${log_date}', flag_type='8')
  select card_no, pass_num, area_code, max_tx_date, max_tx_time
  from (
  select t.card_no, t.pass_num, t.area_code, max_tx_date, max_tx_time,
         row_number() over (partition by t.card_no, t.pass_num, t.area_code 
                            order by num desc ) rownum
    from (
      select sa_tx_crd_no as card_no, CRDT_NO as pass_num, substr(area_code,1,4) as area_code,
             max(SA_TX_DT) as max_tx_date, max(SA_TX_TM) as max_tx_time,
             count(*) as num
         from main_trade_flow_ccbs_atm_pos_ectip_card_user_a 
        where p9_data_date <= '${log_date}'
          and p9_data_date > ts150.my_date_add('${log_date}', -180)
          and sa_tx_crd_no <> ''
          and CRDT_NO <> ''
          and area_code <> ''
        group by sa_tx_crd_no, CRDT_NO, substr(area_code,1,4)
      ) t
   ) a
   where rownum >=3;
!
}

drop_old_data()
{
   drop_date=`date -d "-7 days $log_date" +%Y%m%d`
   echo "stat_tx_last_time_a drop partition ",$drop_date

   beeline << !
      use $db;
      alter table stat_tx_last_time_a drop IF EXISTS partition(p9_data_date='${drop_date}');
!
}

###调用
run()
{
  fill_today
  fill_yesterday 

  #drop_old_data
}
