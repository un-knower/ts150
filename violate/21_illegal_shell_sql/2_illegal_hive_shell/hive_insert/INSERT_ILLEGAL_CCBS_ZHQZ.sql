use sor;


--通过卡号获取开卡机构
insert overwrite table ILLEGAL_CCBS_ZHQZ_OPENORG partition (p9_data_date = '$p9_data_date')
select distinct a.*, b.CARD_BRANCH
  from (select *
          from p2log_ccbs_res k
         where k.que_date = '$p9_data_date'
           and (card_no IS NOT NULL and card_no <> '')) a
 inner join SIAM_CARD_INFO_H b on a.card_no = b.card_no;
 

--行为异地的查询
insert overwrite table ILLEGAL_CCBS_ZHQZ_NOSAME partition (p9_data_date = '$p9_data_date')
select a.*
  from ILLEGAL_CCBS_ZHQZ_OPENORG a
 where  p9_data_datee = '$p9_data_date'
   and substr(query_org, 1, 3) <> substr(CARD_BRANCH, 1, 3)
   and ((a.query_org IS Not null and a.query_org <> '') or
        (a.CARD_BRANCH IS Not null and a.CARD_BRANCH <> ''));




--不在排队机系统的日志信息
insert overwrite table ILLEGAL_CCBS_ZHQZ_NOQUEUE partition (p9_data_date = '$p9_data_date')
select a.*
  from (select * from ILLEGAL_CCBS_ZHQZ_NOSAME where p9_data_date = '$p9_data_date') a
  left join (select * from ILLEGAL_CUST_QUEUE_INFO where p9_data_date = 'p9_data_date') 
    on a.que_date = b.tx_dt
    and a.crdt_no = b.PASS_NUM
 where b.tx_dt IS NULL;



--获取客户卡开户行机构名称
insert overwrite table ILLEGAL_CCBS_ZHQZ_CARD_BRANCH partition (p9_data_date = '$p9_data_date')
select a.*, b.ccbins_chn_shrtnm as card_branch_nm
  from (select * from  ILLEGAL_CCBS_ZHQZ_NOSAME  where p9_data_date = '$p9_data_date' ) a
 inner join T0651_CCBINS_INF_H b 
   on a.CARD_BRANCH = b.ccbins_id
   and b.P9_END_DATE = '2999-12-31';



--获取添加柜员身份证号
insert overwrite table ILLEGAL_CCBS_ZHQZ_OPER_PASSNUM partition (p9_data_date = '$p9_data_date')
select a.*, b.CM_id_NO
  from (select * from ILLEGAL_CCBS_ZHQZ_CARD_BRANCH p9_data_date='$p9_data_date') a
 inner join TODDC_FCMTLR0_H b 
  on a.tlr_id = b.cm_opr_no
  and b.P9_END_DATE = '2999-12-31';



--获取员工名称与员工所在机构名称
insert overwrite table ILLEGAL_CCBS_ZHQZ_OPER_INFO partition (p9_data_date = '$p9_data_date')
select a.*, b.CM_OPR_NAME, b.CM_OPUN_CODE, c.ccbins_chn_shrtnm
  from (select * from ILLEGAL_CCBS_ZHQZ_OPER_PASSNUM where p9_data_date='$p9_data_date') a
 inner join TODDC_FCMTLR0_H b 
   on a.tlr_id = b.CM_OPR_NO
   and b.P9_END_DATE = '2999-12-31'
 inner join T0651_CCBINS_INF_H c 
   on b.CM_OPUN_CODE = c.ccbins_id
   and c.P9_END_DATE = '2999-12-31');


--获取父机构号
insert overwrite table ILLEGAL_CCBS_ZHQZ_OPER_PARENT partition (p9_data_date = '$p9_data_date')
select a.que_date,
       a.que_time,
       a.tlr_id,
       a.CM_id_NO,
       a.CM_OPR_NAME,
       a.query_org,
       a.CM_OPUN_CODE,
       a.ccbins_chn_shrtnm,
       b.parent_id,
       b.ccbins_chn_shrtnm as parent_chn_shrtnm,
       a.card_no,
       a.CARD_BRANCH,
       a.card_branch_nm,
       a.cust_no,
       a.crdt_no
  from (select * from ILLEGAL_CCBS_ZHQZ_OPER_INFO where  p9_data_date='$p9_data_date') a
 inner join ccbins_provice b 
   on a.CM_OPUN_CODE = b.scdy_ccbins_id;


--过滤掉没有在ATM、签约等交易的
insert overwrite table ILLEGAL_CCBS_ZHQZ_NOTXN partition (p9_data_date = '$p9_data_date')
select a.*
  from ( select * from ILLEGAL_CCBS_ZHQZ_OPER_INFO where p9_data_date='$p9_data_date')  a
  left join Refine1Detail b 
    on a.que_date = b.tx_dt
    and a.crdt_no = b.PASS_NUM
    and a.query_org = b.branch_id
 where b.tx_dt IS NULL;



--按日期统计
insert overwrite table ILLEGAL_CCBS_ZHQZ_A partition (p9_data_date = '$p9_data_date')
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
  from ILLEGAL_CCBS_ZHQZ_NOTXN a
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

