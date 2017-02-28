use sor;

--查询无卡信息
insert overwrite table ILLEGAL_P2NOCARD partition (p9_data_date = '$p9_data_date')
select a.tx_date,
       a.tx_time,
       a.tx_code,
       a.OPER_CODE,
       a.OPER_NAME,
       a.TERMINAL_MAC,
       a.TERMINAL_IP,
       a.org_short_cname,
       a.inst_level1_branch_id,
       a.BRANCH_ID,
       a.CST_ID,
       a.FS_ACCT_NO,
       a.Id_Crdt_No
  from p2log_cst_query a
where substr(a.tx_code,1,9) in ('A00421502', 'A00421517', 'A00421538', 'A00421547')
  and a.tx_date = '$p9_data_date'
  and (a.customer_identity_no IS NULL or a.customer_identity_no = '')
  and (a.customer_acctount_no IS NULL or a.customer_acctount_no = '');

   
--通过 卡号、身份证、客户号 补全信息
insert overwrite table ILLEGAL_P2NOCARD_CUST_INFO partition (p9_data_date = '$p9_data_date')
select a.tx_date,
       a.tx_time,
       a.tx_code,
       a.OPER_CODE,
       a.OPER_NAME,
       a.TERMINAL_MAC,
       a.TERMINAL_IP,
       a.org_short_cname,
       a.inst_level1_branch_id,
       a.BRANCH_ID,
       a.CST_ID,
       b.pass_num,
       a.FS_ACCT_NO,
       b.card_no
  from (select *
          from ILLEGAL_P2NOCARD  k
         where k.p9_data_date = '$p9_data_date'
           and k.FS_ACCT_NO IS NOT NULL
           and k.FS_ACCT_NO <> '') a
 inner join SIAM_CARD_INFO_H b on a.FS_ACCT_NO = b.card_no

union

select a.tx_date,
       a.tx_time,
       a.tx_code,
       a.OPER_CODE,
       a.OPER_NAME,
       a.TERMINAL_MAC,
       a.TERMINAL_IP,
       a.org_short_cname,
       a.inst_level1_branch_id,
       a.BRANCH_ID,
       a.CST_ID,
       b.pass_num,
       a.FS_ACCT_NO,
       b.card_no
  from (select *
          from ILLEGAL_P2NOCARD k
         where k.p9_data_date = '$p9_data_date' 
          and (k.FS_ACCT_NO IS NULL or k.FS_ACCT_NO = '')
           and (k.Id_Crdt_No IS NOT NULL and k.Id_Crdt_No <> '')) a
 inner join SIAM_CARD_INFO_H b on a.Id_Crdt_No = b.pass_num

union

select a.tx_date,
       a.tx_time,
       a.tx_code,
       a.OPER_CODE,
       a.OPER_NAME,
       a.TERMINAL_MAC,
       a.TERMINAL_IP,
       a.org_short_cname,
       a.inst_level1_branch_id,
       a.BRANCH_ID,
       a.CST_ID,
       c.pass_num,
       a.FS_ACCT_NO,
       c.card_no
  from (select *
          from ILLEGAL_P2NOCARD k
         where k.p9_data_date = '$p9_data_date'
           and (k.FS_ACCT_NO IS NULL or k.FS_ACCT_NO = '')
           and (k.Id_Crdt_No IS NULL or k.Id_Crdt_No = '')
           and k.Cst_ID IS NOT NULL and k.Cst_ID <> '') a
 inner join T0042_TBPC1010_H b on a.CST_ID = b.CST_ID
 inner join SIAM_CARD_INFO_H c on b.CRDT_NO = c.pass_num;



--通过银行卡号，获取银行卡开户机构
--获取无卡查询机构对比信息
insert overwrite table ILLEGAL_P2NOCARD_OPUN partition (p9_data_date = '$p9_data_date')
select a.tx_date,
       a.tx_time,
       a.tx_code,
       a.OPER_CODE,
       a.OPER_NAME,
       a.TERMINAL_MAC,
       a.TERMINAL_IP,
       a.org_short_cname,
       a.inst_level1_branch_id,
       a.BRANCH_ID,
       a.CST_ID,
       a.pass_num,
       a.FS_ACCT_NO,
       a.card_no,
       
       b.CR_OPUN_COD,
       (case
         when (substr(a.INST_LEVEL1_BRANCH_ID, 1, 3) =
              substr(b.CR_OPUN_COD, 1, 3) or
              substr(a.BRANCH_ID, 1, 3) = substr(b.CR_OPUN_COD, 1, 3)) then
          1
         else
          0
       end) as same
  from (select *
          from ILLEGAL_P2NOCARD_CUST_INFO
         where p9_data_date = '$p9_data_date') a
 inner join TODDC_CRCRDCRD_H b on A.card_no = b.CR_CRD_NO;


--获取异地无卡查询的客户
insert overwrite table ILLEGAL_P2NOCARD_NOSAME_PASSNUM partition (p9_data_date = '$p9_data_date')
select a.tx_date,
       a.tx_time,
       a.OPER_CODE,
       a.pass_num,
       sum(same) as is_same
  from ILLEGAL_P2NOCARD_OPUN a
 where p9_data_date = '$p9_data_date'
   and a.oper_code IS Not null
   and a.oper_code <> ''
 group by a.tx_date, a.tx_time, a.OPER_CODE, a.pass_num
having sum(same) = 0;



--获取异地无卡查询的明细
insert overwrite table ILLEGAL_P2NOCARD_NOSAME_A partition (p9_data_date = '$p9_data_date')
select a.tx_date,
       a.tx_time,
       a.tx_code,
       a.OPER_CODE,
       a.OPER_NAME,
       a.TERMINAL_MAC,
       a.TERMINAL_IP,
       a.org_short_cname,
       a.BRANCH_ID,
       a.CST_ID,
       a.pass_num,
       a.FS_ACCT_NO,
       a.card_no,
       a.CR_OPUN_COD
  from (select * from ILLEGAL_P2NOCARD_OPUN where p9_data_date='$p9_data_date') a
 inner join (select * from ILLEGAL_P2NOCARD_NOSAME_PASSNUM where p9_data_date='$p9_data_date') b 
 on a.tx_date = b.tx_date
    and a.tx_time = b.tx_time
    and a.OPER_CODE = b.OPER_CODE
    and a.pass_num = b.pass_num;



--获取异地无卡查询的明细+客户机构信息
insert overwrite table ILLEGAL_P2NOCARD_NOSAME_A_CUSTORG partition (p9_data_date = '$p9_data_date')
select  a.tx_date,
       a.tx_time,
       a.tx_code,
       a.OPER_CODE,
       a.OPER_NAME,
       a.TERMINAL_MAC,
       a.TERMINAL_IP,
       a.org_short_cname,
       a.BRANCH_ID,
       a.CST_ID,
       a.pass_num,
       a.FS_ACCT_NO,
       a.card_no,
       a.CR_OPUN_COD,
        b.ccbins_chn_shrtnm as CR_OPUN_NM
  from (select * from ILLEGAL_P2NOCARD_NOSAME_A where p9_data_date = '$p9_data_date') a
 inner join T0651_CCBINS_INF_H b on a.CR_OPUN_COD = b.ccbins_id;



--获取异地无卡查询的明细+柜员机构信息
insert overwrite table ILLEGAL_P2NOCARD_NOSAME_A_OPERORG partition (p9_data_date = '$p9_data_date')
select a.tx_date,
       a.tx_time,
       a.tx_code,
       a.OPER_CODE,
       a.OPER_NAME,
       a.TERMINAL_MAC,
       a.TERMINAL_IP,
       a.org_short_cname,
       a.BRANCH_ID,
       a.CST_ID,
       a.pass_num,
       a.FS_ACCT_NO,
       a.card_no,
       a.CR_OPUN_COD,
       a.CR_OPUN_NM,
       b.USR_NM as CM_OPR_NAME,
       b.BLNG_INSID as CM_OPUN_CODE,
       c.ccbins_chn_shrtnm,
       d.parent_id,
       d.ccbins_chn_shrtnm as parent_chn_shrtnm
  from (select * from ILLEGAL_P2NOCARD_NOSAME_A_CUSTORG where p9_data_date = '$p9_data_date') a
 inner join T0861_EMPE_H b on OPER_CODE = b.CCB_EMPID
     and b.P9_END_DATE = '2999-12-31'
 inner join T0651_CCBINS_INF_H c on b.BLNG_INSID = c.ccbins_id
     and c.P9_END_DATE = '2999-12-31'
 inner join ccbins_provice d on b.BLNG_INSID = d.scdy_ccbins_id;


--统计信息
insert overwrite table ILLEGAL_P2NOCARD_A partition (p9_data_date = '$p9_data_date')
select a.tx_date,
       a.OPER_CODE,
       a.CM_OPR_NAME,
       a.CM_OPUN_CODE,
       a.ccbins_chn_shrtnm,
       a.parent_id,
       a.parent_chn_shrtnm,
       count(distinct a.pass_num) as num
  from (select * from ILLEGAL_P2NOCARD_NOSAME_A_OPERORG where p9_data_date = '$p9_data_date') a
 group by a.tx_date,
          a.OPER_CODE,
          a.CM_OPR_NAME,
          a.CM_OPUN_CODE,
          a.ccbins_chn_shrtnm,
          a.parent_id,
          a.parent_chn_shrtnm
 order by num desc;





----删除临时表信息
ALTER TABLE ILLEGAL_P2NOCARD DROP IF EXISTS PARTITION(p9_data_date='$p9_data_date');
ALTER TABLE ILLEGAL_P2NOCARD_CUST_INFO DROP IF EXISTS PARTITION(p9_data_date='$p9_data_date');
ALTER TABLE ILLEGAL_P2NOCARD_OPUN DROP IF EXISTS PARTITION(p9_data_date='$p9_data_date');
ALTER TABLE ILLEGAL_P2NOCARD_NOSAME_PASSNUM DROP IF EXISTS PARTITION(p9_data_date='$p9_data_date');
ALTER TABLE ILLEGAL_P2NOCARD_NOSAME_A DROP IF EXISTS PARTITION(p9_data_date='$p9_data_date');
ALTER TABLE ILLEGAL_P2NOCARD_NOSAME_A_CUSTORG DROP IF EXISTS PARTITION(p9_data_date='$p9_data_date');
ALTER TABLE ILLEGAL_P2NOCARD_NOSAME_A_OPERORG DROP IF EXISTS PARTITION(p9_data_date='$p9_data_date');
