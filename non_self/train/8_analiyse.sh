#!/bin/sh

#引用基础Shell函数库
source /home/ap/dip_ts150/ts150_script/base.sh

#解释命令行参数
#logdate_arg $*

#新集群登陆
#hadoop_login

db=verify_2

batch=0327

p9_data_date=20170327

get_detail()
{
data_date=$1

beeline <<!
use ${db};

 drop table if exists main_trade_flow_ccbs_atm_pos_ectip_card_user_a_${batch}_${data_date};

 create table main_trade_flow_ccbs_atm_pos_ectip_card_user_a_${batch}_${data_date} as
 select a.predict, b.*
   from 
      ( select split(id, '_')[0] as sa_acct_no, split(id, '_')[1] as sa_ddp_acct_no_det_n, predict
          from train_feature_trad_flow_a_mark_predict_${batch}_${data_date} 
      ) a
  inner join 
     ( select *
         from main_trade_flow_ccbs_atm_pos_ectip_card_user_a
   --     where p9_data_date='${data_date}'
     ) b
    on a.sa_acct_no = b.sa_acct_no
   and a.sa_ddp_acct_no_det_n = b.sa_ddp_acct_no_det_n;

!
}

get_mark_detail()
{
data_date=$1

beeline <<!
use mid;

 drop table if exists main_trade_flow_ccbs_atm_pos_ectip_card_user_a_mark;

 create table main_trade_flow_ccbs_atm_pos_ectip_card_user_a_mark as
     select a.*
       from main_trade_flow_ccbs_atm_pos_ectip_card_user_a a
  inner join 
      ( select split(flow_no, '_')[0] as sa_acct_no, split(flow_no, '_')[1] as sa_ddp_acct_no_det_n
          from mark_flow_no_201612
      ) b
    on a.sa_acct_no = b.sa_acct_no
   and a.sa_ddp_acct_no_det_n = b.sa_ddp_acct_no_det_n;

  INSERT OVERWRITE DIRECTORY '/bigdata/output/TS150/nonauth/mark_detail_${data_date}'
   ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' WITH SERDEPROPERTIES('serialization.null.format'='')
   SELECT * from main_trade_flow_ccbs_atm_pos_ectip_card_user_a_mark
    order by crdt_no, sa_tx_crd_no, sa_tx_dt, sa_tx_tm
        ;
!
}

analiyse_1()
{
beeline <<!
use $db;

 drop table if exists train_feature_trad_flow_a_${batch};

 create table train_feature_trad_flow_a_${batch} as
 select a.predict, b.*
   from 
      ( select split(id, '_')[0] as sa_acct_no, split(id, '_')[1] as sa_ddp_acct_no_det_n, predict
          from train_feature_trad_flow_a_mark_predict_${batch}_${data_date}
      ) a
  inner join 
     ( select *
         from ${db}.train_feature_trad_flow_a
        where p9_data_date='${data_date}'
     ) b
    on a.sa_acct_no = b.sa_acct_no
   and a.sa_ddp_acct_no_det_n = b.sa_ddp_acct_no_det_n;

 select chanl_type, count(*) as num, max(sa_tx_amt) as max_amt, 
        min(sa_tx_amt) as min_amt, avg(sa_tx_amt) as avg_amt, 
        max(sa_ddp_acct_bal) as max_bal, min(sa_ddp_acct_bal) as min_bal,
        avg(sa_ddp_acct_bal) as avg_bal
   from train_feature_trad_flow_a_${batch}
  group by chanl_type;

 

!
}
analiyse_2()
{
beeline <<!
use $db;

 select count(*) as num, count(distinct sa_tx_crd_no) as num_card,
        count(distinct crdt_no) as num_pass,
        max(cast(sa_tx_amt as doubal)) as max_amt, 
        min(cast(sa_tx_amt as doubal)) as min_amt, 
        avg(cast(sa_tx_amt as doubal)) as avg_amt, 
        max(sa_ddp_acct_bal) as max_bal, min(sa_ddp_acct_bal) as min_bal,
        avg(sa_ddp_acct_bal) as avg_bal
   from main_trade_flow_ccbs_atm_pos_ectip_card_user_a_${batch};

 

!
}

# top 10 card
analiyse_top10()
{
data_date=$1

beeline <<!
use $db;

 drop table if exists export_10card_temp;

 create table export_10card_temp as
   select distinct predict, sa_tx_crd_no, sa_acct_no, sa_ddp_acct_no_det_n
     from main_trade_flow_ccbs_atm_pos_ectip_card_user_a_${batch}_${data_date}
    where predict = 1.0
      and substring(CR_OPUN_COD,1,3) = '351'
    limit 10;

 insert into table export_10card_temp 
   select distinct predict, sa_tx_crd_no, sa_acct_no, sa_ddp_acct_no_det_n
     from main_trade_flow_ccbs_atm_pos_ectip_card_user_a_${batch}_${data_date}
    where predict = 1.0
      and substring(CR_OPUN_COD,1,3) = '510'
    limit 10;

 drop table if exists export_10card_all_trad_flow_a_${batch}_${data_date};

 create table export_10card_all_trad_flow_a_${batch}_${data_date} as
   select b.*
     from export_10card_temp a
    inner join main_trade_flow_ccbs_atm_pos_ectip_card_user_a b
       on a.sa_tx_crd_no = b.sa_tx_crd_no;
!
}

export_hdfs()
{
data_date=$1
run_date='2017${batch}'

beeline <<!
use $db;

  INSERT OVERWRITE DIRECTORY '/bigdata/output/TS150/nonauth/top10_trad_flow_${batch}_${data_date}'
   ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' WITH SERDEPROPERTIES('serialization.null.format'='')
   SELECT * from export_10card_all_trad_flow_a_${batch}_${data_date}
    order by sa_acct_no, sa_ddp_acct_no_det_n;

  INSERT OVERWRITE DIRECTORY '/bigdata/output/TS150/nonauth/top10_card_${batch}_${data_date}'
   ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' WITH SERDEPROPERTIES('serialization.null.format'='')
   SELECT * from export_10card_temp;

  INSERT OVERWRITE DIRECTORY '/bigdata/output/TS150/nonauth/predict90_card_351_${batch}_${data_date}'
   ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' WITH SERDEPROPERTIES('serialization.null.format'='')
   SELECT distinct sa_tx_crd_no from main_trade_flow_ccbs_atm_pos_ectip_card_user_a_${batch}_${data_date}
   where predict>=0.90
     and substring(SA_OPUN_COD,1,3) = '351';

  INSERT OVERWRITE DIRECTORY '/bigdata/output/TS150/nonauth/predict90_card_510_${batch}_${data_date}'
   ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' WITH SERDEPROPERTIES('serialization.null.format'='')
   SELECT distinct sa_tx_crd_no from main_trade_flow_ccbs_atm_pos_ectip_card_user_a_${batch}_${data_date}
   where predict>=0.90
     and substring(SA_OPUN_COD,1,3) = '510';

  INSERT OVERWRITE DIRECTORY '/bigdata/output/TS150/nonauth/predict99_card_351_${batch}_${data_date}'
   ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' WITH SERDEPROPERTIES('serialization.null.format'='')
   SELECT distinct sa_tx_crd_no from main_trade_flow_ccbs_atm_pos_ectip_card_user_a_${batch}_${data_date}
   where predict>=0.99
     and substring(SA_OPUN_COD,1,3) = '351';

  INSERT OVERWRITE DIRECTORY '/bigdata/output/TS150/nonauth/predict99_card_510_${batch}_${data_date}'
   ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' WITH SERDEPROPERTIES('serialization.null.format'='')
   SELECT distinct sa_tx_crd_no from main_trade_flow_ccbs_atm_pos_ectip_card_user_a_${batch}_${data_date}
   where predict>=0.99
     and substring(SA_OPUN_COD,1,3) = '510';

  INSERT OVERWRITE DIRECTORY '/bigdata/output/TS150/nonauth/predict90_detail_351_${batch}_${data_date}'
   ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' WITH SERDEPROPERTIES('serialization.null.format'='')
   SELECT * from main_trade_flow_ccbs_atm_pos_ectip_card_user_a_${batch}_${data_date}
   where predict>=0.90
     and substring(SA_OPUN_COD,1,3) = '351';

  INSERT OVERWRITE DIRECTORY '/bigdata/output/TS150/nonauth/predict90_detail_510_${batch}_${data_date}'
   ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' WITH SERDEPROPERTIES('serialization.null.format'='')
   SELECT * from main_trade_flow_ccbs_atm_pos_ectip_card_user_a_${batch}_${data_date}
   where predict>=0.90
     and substring(SA_OPUN_COD,1,3) = '510';
!

}

export_hdfs_2()
{
data_date=$1
run_date='2017${batch}'

beeline <<!
use $db;

--  INSERT OVERWRITE DIRECTORY '/bigdata/output/TS150/nonauth/top10_trad_flow_${batch}_${data_date}'
--   ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' WITH SERDEPROPERTIES('serialization.null.format'='')
--   SELECT * from export_10card_all_trad_flow_a_${batch}_${data_date}
--    order by sa_acct_no, sa_ddp_acct_no_det_n;

--  INSERT OVERWRITE DIRECTORY '/bigdata/output/TS150/nonauth/top10_card_${batch}_${data_date}'
--  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' WITH SERDEPROPERTIES('serialization.null.format'='')
--   SELECT * from export_10card_temp;

  INSERT OVERWRITE DIRECTORY '/bigdata/output/TS150/nonauth/predict90_card_65_${batch}_${data_date}'
   ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' WITH SERDEPROPERTIES('serialization.null.format'='')
   SELECT distinct sa_tx_crd_no from main_trade_flow_ccbs_atm_pos_ectip_card_user_a_${batch}_${data_date}
   where predict>=0.90
     and substring(CR_OPUN_COD,1,2) = '65';

  INSERT OVERWRITE DIRECTORY '/bigdata/output/TS150/nonauth/predict90_card_442_${batch}_${data_date}'
   ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' WITH SERDEPROPERTIES('serialization.null.format'='')
   SELECT distinct sa_tx_crd_no from main_trade_flow_ccbs_atm_pos_ectip_card_user_a_${batch}_${data_date}
   where predict>=0.90
     and substring(SA_OPUN_COD,1,3) = '442';

  INSERT OVERWRITE DIRECTORY '/bigdata/output/TS150/nonauth/predict99_card_65_${batch}_${data_date}'
   ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' WITH SERDEPROPERTIES('serialization.null.format'='')
   SELECT distinct sa_tx_crd_no from main_trade_flow_ccbs_atm_pos_ectip_card_user_a_${batch}_${data_date}
   where predict>=0.99
     and substring(CR_OPUN_COD,1,2) = '65';

  INSERT OVERWRITE DIRECTORY '/bigdata/output/TS150/nonauth/predict99_card_442_${batch}_${data_date}'
   ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' WITH SERDEPROPERTIES('serialization.null.format'='')
   SELECT distinct sa_tx_crd_no from main_trade_flow_ccbs_atm_pos_ectip_card_user_a_${batch}_${data_date}
   where predict>=0.99
     and substring(SA_OPUN_COD,1,3) = '442';

  INSERT OVERWRITE DIRECTORY '/bigdata/output/TS150/nonauth/predict90_detail_65_${batch}_${data_date}'
   ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' WITH SERDEPROPERTIES('serialization.null.format'='')
   SELECT * from main_trade_flow_ccbs_atm_pos_ectip_card_user_a_${batch}_${data_date}
   where predict>=0.90
     and substring(CR_OPUN_COD,1,2) = '65';

  INSERT OVERWRITE DIRECTORY '/bigdata/output/TS150/nonauth/predict90_detail_442_${batch}_${data_date}'
   ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' WITH SERDEPROPERTIES('serialization.null.format'='')
   SELECT * from main_trade_flow_ccbs_atm_pos_ectip_card_user_a_${batch}_${data_date}
   where predict>=0.90
     and substring(SA_OPUN_COD,1,3) = '442';
!

}
export_table()
{
data_date=$1
table=$2
run_date='2017${batch}'


beeline <<!
use $db;

  INSERT OVERWRITE DIRECTORY '/bigdata/output/TS150/nonauth/${table}_${batch}_${data_date}'
   ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' WITH SERDEPROPERTIES('serialization.null.format'='')
   --SELECT * from ${table}_${batch}_${data_date};
   SELECT * from ${table};

!
}

download_hdfs()
{
data_date=$1
table=$2
run_date=2017${batch}

localDir=/home/ap/dip/file/archive/output/ts150/000000000/data/oodid
localFilePath=$localDir/${run_date}
localFileName=$localFilePath/TS150_SAFEID_000000000_${table}_${batch}_${data_date}_0001.dat


hadoop fs -copyToLocal /bigdata/output/TS150/nonauth/${table}_${batch}_${data_date} ${localFilePath}
#hadoop fs -copyToLocal /bigdata/output/TS150/nonauth/${table}_${data_date} ${localFilePath}

sed -e "s//|@|/g" ${localFilePath}/${table}_${batch}_${data_date}/0* | sed -e "s/\\\N//g" > ${localFileName}
#sed -e "s//|@|/g" ${localFilePath}/${table}_${data_date}/0* | sed -e "s/\\\N//g" > ${localFileName}

rm -rf ${localFilePath}/0*

chmod -R a+rw ${localFilePath}

return 0

}

run()
{
  data_date=$1

  get_detail $data_date
  #analiyse_top10 $data_date
  #export_hdfs $data_date
  export_hdfs_2 $data_date
  #download_hdfs $data_date top10_trad_flow
  #download_hdfs $data_date top10_card
  #download_hdfs $data_date predict90_card_351
  #download_hdfs $data_date predict90_card_510
  #download_hdfs $data_date predict99_card_351
  #download_hdfs $data_date predict99_card_510
  #download_hdfs $data_date predict90_detail_351
  #download_hdfs $data_date predict90_detail_510
  download_hdfs $data_date predict90_card_65
  download_hdfs $data_date predict90_card_442
  download_hdfs $data_date predict99_card_65
  download_hdfs $data_date predict99_card_442
  download_hdfs $data_date predict90_detail_65
  download_hdfs $data_date predict90_detail_442
}

run  $p9_data_date
#get_mark_detail  $p9_data_date
#download_hdfs $p9_data_date mark_detail
#export_table $p9_data_date top1000_crdtno
#download_hdfs $p9_data_date top1000_crdtno
