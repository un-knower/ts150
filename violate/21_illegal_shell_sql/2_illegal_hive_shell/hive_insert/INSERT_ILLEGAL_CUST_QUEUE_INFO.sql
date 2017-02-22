use sor;


--通过卡号、ID取得排队信息中客户的身份证号

insert overwrite table ILLEGAL_CUST_QUEUE_INFO_TMP partition (p9_data_date = '$p9_data_date')
select a.cst_ofrnum_dt as TX_DT, b.pass_num
  from T0162_CUST_QUEUE_INFO_A a
 inner join SIAM_CARD_INFO_H b 
   on a.srcsys_ar_id = b.card_no
   and a.cst_ofrnum_dt = '$p9_data_date')
union
select a.cst_ofrnum_dt as TX_DT,
       substr(a.srcsys_ar_id, 3, length(a.srcsys_ar_id) - 2) as pass_num
  from T0162_CUST_QUEUE_INFO_A a
 where substr(a.srcsys_ar_id, 1, 2) = 'id'
   and a.cst_ofrnum_dt = '$p9_data_date';
   

--新老系统结合
insert overwrite table ILLEGAL_CUST_QUEUE_INFO partition (p9_data_date = '$p9_data_date')
select distinct *
  from CUST_QUEUE_INFO_A_tmp
union
select  * from T05_Card_PASS_NUM;


