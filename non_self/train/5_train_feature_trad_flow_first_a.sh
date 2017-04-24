#!/bin/sh

#引用基础Shell函数库
source /home/ap/dip_ts150/ts150_script/ccb_risk_scoring/base.sh

#解释命令行参数
logdate_arg $*

#新集群登陆
hadoop_login

db=train

IN_CUR_HIVE="main_trade_flow_ccbs_atm_pos_ectip_card_user_a"
IN_PRE_HIVE="train_feature_trad_flow_first_a stat_card_tx_last_time_h stat_pass_tx_last_time_h"
OUT_CUR_HIVE="train_feature_trad_flow_first_a"

#设置日期（当天，前7天，前30天，前90天）
p9_data_date=$log_date

less_1_day=`date -d "$p9_data_date 1 days ago" +"%Y%m%d"`

#插入p9_data_date当天卡统计信息
export_feature_trad_last(){

beeline <<!
use $db;

set role admin;

add jar hdfs://hacluster/bigdata/common/function/ts150_hive_tools.jar;
add jar hdfs://hacluster/bigdata/common/function/siam-etl-p2log-hive_udf.jar;

create function ts150.ts150_GetNotEmpty as 'com.ccb.ts150.p2log.GetNotEmpty_UDF' using jar 'hdfs://hacluster/bigdata/common/function/siam-etl-p2log-hive_udf.jar';


create function ts150.my_isfirsttx as 'com.ccb.ts150.hive_udf.MyIsFirstTx' using jar 'hdfs://hacluster/bigdata/common/function/ts150_hive_tools.jar';

create function ts150.my_lasttxdate as 'com.ccb.ts150.hive_udf.MyLastDate' using jar 'hdfs://hacluster/bigdata/common/function/ts150_hive_tools.jar';


--set 

insert overwrite table train_feature_trad_flow_first_a_tmp PARTITION(p9_data_date='$p9_data_date')
select a.SA_TX_CRD_NO,a.crdt_no, SA_ACCT_NO,
       SA_DDP_ACCT_NO_DET_N,
       case when REGEXP(SA_OPR_NO,'^\\d+$') then '1'
            when CR_ATM_NO is not null then '2'
            when CR_POS_REF_NO is not null then '3'
            when (CHANL_NO='01' or CHANL_NO='0003' )then '4'
            when CHANL_NO='02' then '5'
            else '6' 
        end as CHANL_TYPE,

       SA_APP_TX_CODE,
       SA_TX_AMT,
       SA_DDP_ACCT_BAL,
       SA_DSCRP_COD,
       SA_SVC,
       SA_EC_FLG,

       '' tx_last_time,

 
       case when SA_CR_AMT>0 then 1 else 2 end,
       case when SA_OP_ACCT_NO_32 is not null then 1 else 2 end,
        


       if(ts150.my_isfirsttx(a.SA_TX_DT,ts150.my_lasttxdate(b.flag,'2',a.SA_APP_TX_CODE),365),2,1),
       if(ts150.my_isfirsttx(a.SA_TX_DT,ts150.my_lasttxdate(b.flag,'1',
                             ts150.ts150_GetNotEmpty(CR_ATM_NO, CR_POS_REF_NO, mobile, term_qry)),90),2,1),
       if(ts150.my_isfirsttx(a.SA_TX_DT,ts150.my_lasttxdate(b.flag,'3',a.SA_OP_ACCT_NO_32),180),2,1),
       if(ts150.my_isfirsttx(a.SA_TX_DT,ts150.my_lasttxdate(b.flag,'4',a.SA_OPUN_COD),90),2,1),
       if(ts150.my_isfirsttx(a.SA_TX_DT,ts150.my_lasttxdate(b.flag,'5',a.IP),90),2,1),
       if(ts150.my_isfirsttx(a.SA_TX_DT,ts150.my_lasttxdate(b.flag,'8',a.area_code),180),2,1),

 
       case when length(SA_OP_BANK_NO)=9 then '1' else '2' end,
 
       case when a.sa_cust_name=a.SA_OP_CUST_NAME then '1' else '2' end,
       

       '','','','','','',

       '0' as ASS_ACCT_DR_TIME,
       a.SA_TX_DT ,
       a.SA_TX_TM ,
       a.CR_ATM_NO ,
       a.CR_POS_REF_NO ,
       a.mobile ,
       a.term_qry ,
       a.SA_OP_ACCT_NO_32 ,
       a.SA_OPUN_COD ,
       a.ip ,
       a.area_code
from(
    select * from main_trade_flow_ccbs_atm_pos_ectip_card_user_a
     where  p9_data_date='$p9_data_date' 
    ) a
 
left outer join (
     select card_no, flag_collect as flag
      from stat_card_tx_last_time_h
     where p9_data_date='${less_1_day}'
     ) b
  on a.SA_TX_CRD_NO=b.CARD_NO;



insert overwrite table train_feature_trad_flow_first_a PARTITION(p9_data_date='$p9_data_date')
select a.caRD_NO,a.crdt_no, SA_ACCT_NO,
       SA_DDP_ACCT_NO_DET_N,
       a.CHANL_TYPE,

       SA_APP_TX_CODE,
       SA_TX_AMT,
       SA_DDP_ACCT_BAL,
       SA_DSCRP_COD,
       SA_SVC,
       SA_EC_FLG,

       case when ts150.my_lasttxdate(c.flag,'6','6') is not null then(
            unix_timestamp(concat(substr(a.SA_TX_DT,1,4),'-',substr(a.SA_TX_DT,5,2),'-', substr(a.SA_TX_DT,7,2),' ',
                                  substr(a.SA_TX_TM,1,2),':',substr(a.SA_TX_TM,3,2),':',substr(a.SA_TX_TM,5,2)))
            -
            unix_timestamp(ts150.my_lasttxdate(c.flag,'6','6'))
            ) else -1 
       end as tx_last_time,

       JD_FLAG ,
       IS_ZZ ,

       IS_CARD_JYLX_FIRST ,
       IS_CARD_SB_FIRST ,
       IS_CARD_ZZ_FIRST ,
       IS_CARD_JG_FIRST ,
       IS_CARD_IP_FIRST ,
       IS_CARD_AREA_FIRST ,
 
       IS_KH ,
       IS_BR ,

       if(ts150.my_isfirsttx(a.SA_TX_DT,ts150.my_lasttxdate(c.flag,'2',a.SA_APP_TX_CODE),365),2,1),
       if(ts150.my_isfirsttx(a.SA_TX_DT,ts150.my_lasttxdate(c.flag,'1',
                            ts150.ts150_GetNotEmpty(CR_ATM_NO, CR_POS_REF_NO, mobile, term_qry)),90),2,1),
       if(ts150.my_isfirsttx(a.SA_TX_DT,ts150.my_lasttxdate(c.flag,'3',a.SA_OP_ACCT_NO_32),180),2,1),
       if(ts150.my_isfirsttx(a.SA_TX_DT,ts150.my_lasttxdate(c.flag,'4',a.SA_OPUN_COD),90),2,1),
       if(ts150.my_isfirsttx(a.SA_TX_DT,ts150.my_lasttxdate(c.flag,'5',a.IP),90),2,1),
       if(ts150.my_isfirsttx(a.SA_TX_DT,ts150.my_lasttxdate(c.flag,'8',a.area_code),180),2,1),

       '0' as ASS_ACCT_DR_TIME
from(
    select * from train_feature_trad_flow_first_a_tmp
     where  p9_data_date='$p9_data_date' 
    ) a
 
left outer join (
     select pass_num, flag_collect as flag
      from stat_pass_tx_last_time_h
     where p9_data_date='${less_1_day}'
     ) c
  on a.CRDT_NO=c.PASS_NUM;






!
}

run()
{
   export_feature_trad_last
}


