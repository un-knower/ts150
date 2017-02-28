use sor;

--通过卡号、身份证补全 身份证信息
insert overwrite table ILLEGAL_PBCS_PASSNUM partition (p9_data_date = '$p9_data_date')
select  a.TX_DT as QUE_DATE,
    a.BRANCH_ID AS QUERY_ORG,
    a.DEBIT_ACCT_NUM,
    a.DEBIT_CUSTOMER_NUM AS CUST_NO,
    a.TX_OPERATOR,
    b.pass_num as Crdt_No, 
    b.card_no
  from (select *
          from T05_PBCS_EVENT k
         where k.Tx_Dt = '$p9_data_date'
           and tx_code = '00010501109600'
           and (Debit_Acct_Num IS NOT NULL and Debit_Acct_Num <> '')) a
 inner join SIAM_CARD_INFO_H b on a.Debit_Acct_Num = b.card_no

union

select a.TX_DT as QUE_DATE,
    a.BRANCH_ID AS QUERY_ORG,
    a.DEBIT_ACCT_NUM,
    a.DEBIT_CUSTOMER_NUM AS CUST_NO,
    a.TX_OPERATOR,
    b.pass_num as Crdt_No, 
    b.card_no
  from (select *
          from T05_PBCS_EVENT k
         where k.Tx_Dt = '$p9_data_date'
           and tx_code = '00010501109600'
           and (Debit_Acct_Num IS NULL or Debit_Acct_Num = '')
           and Debit_Customer_Num IS NOT NULL and Debit_Customer_Num <> ''
           ) a
 inner join SIAM_CARD_INFO_H b 
 on substr(a.Debit_Customer_Num, 2, 18) = b.pass_num;

--根据卡号补全 卡开户机构信息
--根据银行卡主档获取营业单位代码CR_OPUN_COD --->注:这个比SIAM_CARD_INFO_H的开户行准确*

insert overwrite table ILLEGAL_PBCS_OPUN partition (p9_data_date = '$p9_data_date')
select a.QUE_DATE,
    a.QUERY_ORG,
    a.DEBIT_ACCT_NUM,
    a.CUST_NO,
    a.TX_OPERATOR,
    a.CRDT_NO,
    a.card_no, 
    b.CR_OPUN_COD
  from (select * from ILLEGAL_PBCS_PASSNUM where p9_data_date='$p9_data_date') a
 inner join TODDC_CRCRDCRD_H b on a.card_no = b.CR_CRD_NO;
   


--明细添加字段，判断是否异地

insert overwrite table ILLEGAL_PBCS_SAME partition (p9_data_date = '$p9_data_date')
select a.QUE_DATE,
    a.QUERY_ORG,
    a.DEBIT_ACCT_NUM,
    a.CUST_NO,
    a.TX_OPERATOR,
    a.CRDT_NO,
    a.card_no, 
    a.CR_OPUN_COD,
       (case
         when (substr(QUERY_ORG,1,3) = substr(CR_OPUN_COD,1,3)) then
          1
         else
          0
       end) as same
  from (select distinct * from ILLEGAL_PBCS_OPUN where p9_data_date='$p9_data_date') a
 where (a.QUERY_ORG IS Not null and a.QUERY_ORG <> '');



--异地查询的客户信息

insert overwrite table ILLEGAL_PBCS_NOSAME partition (p9_data_date = '$p9_data_date')
select a.que_date,
                substr(a.Tx_Operator, 6, 12) as tlr_id,
                a.crdt_no,
                sum(same) as is_same
  from ILLEGAL_PBCS_SAME a
  where p9_data_date='$p9_data_date'
  and (a.Tx_Operator IS Not null and a.Tx_Operator <> '')
 group by a.que_date, substr(a.Tx_Operator, 6, 12), a.crdt_no
having sum(same) = 0;


--明细 过滤出客户机构和柜员机构异地的信息

insert overwrite table ILLEGAL_PBCS_NOSAME_A partition (p9_data_date = '$p9_data_date')
select distinct a.que_date,
                a.tlr_id,
                b.query_org,
                b.debit_acct_num,
                b.card_no,
                b.cr_opun_cod,
                b.cust_no,
                a.crdt_no
  from (select * from ILLEGAL_PBCS_NOSAME where  p9_data_date='$p9_data_date') a
 inner join (select * from ILLEGAL_PBCS_SAME where  p9_data_date='$p9_data_date' ) b 
     on a.que_date = b.que_date
     and a.tlr_id = substr(b.Tx_Operator, 6, 12)
     and a.crdt_no = b.crdt_no;




--客户不在排队机系统的日志信息

insert overwrite table ILLEGAL_PBCS_NOSAME_NOQUEUE partition (p9_data_date = '$p9_data_date')
select     a.que_date,
                a.tlr_id,
                a.query_org,
                a.debit_acct_num,
                a.card_no,
                a.cr_opun_cod,
                a.cust_no,
                a.crdt_no
  from (select * from ILLEGAL_PBCS_NOSAME_A where p9_data_date='p9_data_date') a
  left join (select * from ILLEGAL_CUST_QUEUE_INFO where p9_data_date='p9_data_date') b 
    on a.que_date = b.tx_dt
   and a.Crdt_No = b.PASS_NUM
 where b.tx_dt IS NULL;


-- 获取开户行机构名称
insert overwrite table ILLEGAL_PBCS_OPUNNAME partition (p9_data_date = '$p9_data_date')
select distinct a.que_date,
                a.tlr_id,
                a.query_org,
                a.debit_acct_num,
                a.card_no,
                a.cr_opun_cod,
                a.cust_no,
                a.crdt_no,
                b.ccbins_chn_shrtnm as cr_opun_nm
  from ( select * from ILLEGAL_PBCS_NOSAME_NOQUEUE  where p9_data_date='$p9_data_date') a
 inner join T0651_CCBINS_INF_H b 
   on a.cr_opun_cod = b.ccbins_id
   and b.P9_END_DATE = '2999-12-31';
   
   
   
--添加柜员身份证号
insert overwrite table ILLEGAL_PBCS_OPER_PASSNUM partition (p9_data_date = '$p9_data_date') 
 select  a.que_date,
                a.tlr_id,
                a.query_org,
                a.debit_acct_num,
                a.card_no,
                a.cr_opun_cod,
                a.cust_no,
                a.crdt_no,
                a.cr_opun_nm,
                b.CM_id_NO
  from (select * from ILLEGAL_PBCS_OPUNNAME  where p9_data_date='$p9_data_date') a
 inner join TODDC_FCMTLR0_H b 
   on a.tlr_id = b.cm_opr_no
   and b.P9_END_DATE = '2999-12-31';
   

-- 获取员工名称，与员工所在网点号及网点名
insert overwrite table ILLEGAL_PBCS_OPER_INFO partition (p9_data_date = '$p9_data_date') 
select a.que_date,
                a.tlr_id,
                a.query_org,
                a.debit_acct_num,
                a.card_no,
                a.cr_opun_cod,
                a.cust_no,
                a.crdt_no,
                a.cr_opun_nm,
                a.CM_id_NO,
                b.CM_OPR_NAME, 
                b.CM_OPUN_CODE, 
                c.ccbins_chn_shrtnm
  from (select * from ILLEGAL_PBCS_OPER_PASSNUM  where p9_data_date='$p9_data_date')  a
 inner join TODDC_FCMTLR0_H b 
   on tlr_id = b.CM_OPR_NO
   and b.P9_END_DATE = '2999-12-31'
 inner join T0651_CCBINS_INF_H c 
   on b.CM_OPUN_CODE = c.ccbins_id
   and c.P9_END_DATE = '2999-12-31';
 
 
-- 获取员工网点的父亲机构
insert overwrite table ILLEGAL_PBCS_OPER_PARENT partition (p9_data_date = '$p9_data_date') 
select  a.que_date,
                a.tlr_id,
                a.query_org,
                a.debit_acct_num,
                a.card_no,
                a.cr_opun_cod,
                a.cust_no,
                a.crdt_no,
                a.cr_opun_nm,
                a.CM_id_NO,
                a.CM_OPR_NAME, 
                a.CM_OPUN_CODE, 
                a.ccbins_chn_shrtnm,
                b.parent_id,
                b.ccbins_chn_shrtnm as parent_chn_shrtnm
  from ( select * from ILLEGAL_PBCS_OPER_INFO  where p9_data_date='$p9_data_date' ) a
 inner join ccbins_provice b on a.CM_OPUN_CODE = b.scdy_ccbins_id;


--过滤客户没做ATM_非账务等交易的明细。
insert overwrite table ILLEGAL_PBCS_NOREFINE partition (p9_data_date = '$p9_data_date') 
select     a.que_date,
                a.tlr_id,
                a.query_org,
                a.debit_acct_num,
                a.card_no,
                a.cr_opun_cod,
                a.cust_no,
                a.crdt_no,
                a.cr_opun_nm,
                a.CM_id_NO,
                a.CM_OPR_NAME, 
                a.CM_OPUN_CODE, 
                a.ccbins_chn_shrtnm,
                a.parent_id,
                a.parent_chn_shrtnm
  from (select * from ILLEGAL_PBCS_OPER_PARENT  where p9_data_date='$p9_data_date' )a
  left join ILLEGAL_REFINE_DETAIL b 
    on a.que_date = b.tx_dt
    and a.crdt_no = b.PASS_NUM
    and a.query_org = b.branch_id
 where b.tx_dt IS NULL;


--按日期统计查询量
insert overwrite table ILLEGAL_PBCS_NOREFINE_A partition (p9_data_date = '$p9_data_date') 
select a.que_date,
       a.tlr_id,
       a.CM_id_NO,
       a.cm_opr_name,
       a.query_org,
       a.cm_opun_code,
       a.ccbins_chn_shrtnm,
       a.parent_id,
       a.parent_chn_shrtnm,
       count(distinct a.crdt_no) as num
  from (select * from ILLEGAL_PBCS_NOREFINE  where p9_data_date='$p9_data_date' ) a
 group by a.que_date,
          a.tlr_id,
          a.CM_id_NO,
          a.cm_opr_name,
          a.query_org,
          a.cm_opun_code,
          a.ccbins_chn_shrtnm,
          a.parent_id,
          a.parent_chn_shrtnm
 order by num desc;

 
 
----删除临时表信息
ALTER TABLE ILLEGAL_PBCS_PASSNUM DROP IF EXISTS PARTITION(p9_data_date='$p9_data_date');
ALTER TABLE ILLEGAL_PBCS_OPUN DROP IF EXISTS PARTITION(p9_data_date='$p9_data_date');
ALTER TABLE ILLEGAL_PBCS_SAME DROP IF EXISTS PARTITION(p9_data_date='$p9_data_date');
ALTER TABLE ILLEGAL_PBCS_NOSAME DROP IF EXISTS PARTITION(p9_data_date='$p9_data_date');
ALTER TABLE ILLEGAL_PBCS_NOSAME_A DROP IF EXISTS PARTITION(p9_data_date='$p9_data_date');
ALTER TABLE ILLEGAL_PBCS_NOSAME_NOQUEUE DROP IF EXISTS PARTITION(p9_data_date='$p9_data_date');
ALTER TABLE ILLEGAL_PBCS_OPUNNAME DROP IF EXISTS PARTITION(p9_data_date='$p9_data_date');
ALTER TABLE ILLEGAL_PBCS_OPER_PASSNUM DROP IF EXISTS PARTITION(p9_data_date='$p9_data_date');
ALTER TABLE ILLEGAL_PBCS_OPER_INFO DROP IF EXISTS PARTITION(p9_data_date='$p9_data_date');
ALTER TABLE ILLEGAL_PBCS_OPER_PARENT DROP IF EXISTS PARTITION(p9_data_date='$p9_data_date');
ALTER TABLE ILLEGAL_PBCS_NOREFINE DROP IF EXISTS PARTITION(p9_data_date='$p9_data_date');
