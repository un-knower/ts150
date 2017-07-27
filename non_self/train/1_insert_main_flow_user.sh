#!/bin/sh

######################################################
#   将Hive上的CT_TMP_TODEC_TRAD_FLOW_A临时表导入到贴源表中
#                   wuzhaohui@tienon.com
######################################################

#引用基础Shell函数库
source /home/ap/dip_ts150/ts150_script/ccb_risk_scoring/base.sh

#解释命令行参数
logdate_arg $*

#新集群登陆
hadoop_login

db=train

in_table="mid.train_trade_flow_a"
OUT_CUR_HIVE="main_trade_flow_ccbs_atm_pos_ectip_card_user_a"


insert_trad_flow()
{

beeline <<!

use ${db};
set role admin;
add jar hdfs:///bigdata/common/function/ts150_hive_tools.jar;
add jar hdfs:///bigdata/common/function/ip.txt;

create temporary function ts150_ip2num as 'com.ccb.ts150.hive_udf.Ip2Num_UDF';
create temporary function ts150_CalcDistance as 'com.ccb.ts150.hive_udf.CalcDistance_UDF';
create temporary function ts150_iplocation as 'com.ccb.ts150.hive_udf.GetIpLocation_UDF';


ALTER TABLE ${OUT_CUR_HIVE} ADD IF NOT EXISTS PARTITION(P9_DATA_DATE='${log_date}');

insert overwrite table ${OUT_CUR_HIVE} partition(p9_data_date='${log_date}')
    select
      a.SA_ACCT_NO,
      a.SA_DDP_ACCT_NO_DET_N,
      a.SA_OPR_NO,
      a.SA_EC_FLG,
      a.SA_EC_DET_NO,
      a.SA_CR_AMT,
      a.SA_DDP_ACCT_BAL,
      a.SA_TX_AMT,
      a.SA_TX_CRD_NO,
      a.SA_TX_TYP,
      a.SA_TX_LOG_NO,
      a.SA_DR_AMT,
      a.SA_SVC,
      a.SA_OPUN_COD,
      a.SA_DSCRP_COD,
      a.SA_RMRK,
      a.SA_TX_TM,
      a.SA_TX_DT,
      a.SA_APP_TX_CODE,
      a.SA_EXT_FLG,
      a.SA_OTX_FLG,
      a.SA_OP_ACCT_NO_32,
      a.SA_OP_CUST_NAME,
      a.SA_RMRK_ETX,
      a.SA_SPV_NO,
      a.SA_OP_BANK_NO,
      a.SA_B2B_B2C_RMRK,
      a.CR_ATM_NO,
      a.CR_TX_DT_ATM,
      a.CR_EC_FLG_ATM,
      a.CR_OPR_EC,
      a.CR_TX_NO_ATM,
      a.CR_TX_AMT_ATM,
      a.CR_TX_TM_ATM,
      a.CR_TX_NETP_NO,
      a.CR_CRD_NO_ATM,
      a.CR_DSCRP_COD,
      a.CR_TRNI_SAV_NO,
      a.CR_TRNO_SAV_NO,
      a.CR_POS_REF_NO,
      a.CR_TX_DT_POS,
      a.CR_EC_FLG_POS,
      a.CR_OPR_NO_POS,
      a.CR_TX_NO_POS,
      a.CR_TX_AMT_POS,
      a.CR_ACCT_NO_POS,
      a.CR_TX_TM_POS,
      a.CR_BRIEF,
      a.CR_TEMP_FLG,
      a.CHANL_NO,
      a.FLAT_TRAD_DATE,
      a.FLAT_TRAD_TIME,
      a.CHANL_CUST_NO,
      a.TRAD_NO,
      a.ITCD_NO,
      a.AMT1,
      a.SVC,
      a.ACCT_NO,
      a.ACCT_NAME,
      a.ASS_ACCT_NO,
      a.ASS_ACCT_TYP,
      a.ASS_ACCT_NAME,
      a.TRAD_BRAN,
      a.ACCT_BRAN,
      a.ASS_ACCT_BRAN,
      a.RETURN_CODE,
      a.RETURN_MSG,
      a.TRAD_TLR,
      a.MOBILE,
      a.IP,
      a.CUST_MGR,
      a.SIGN_BRAN,
      a.TRAD_STS,
      a.FLAT_TRAD_DATE_LOGIN,
      a.CUST_NO,
      a.FLAT_TRAD_TIME_LOGIN,
      a.RETURN_CODE_LOGIN,
      a.RETURN_MSG_LOGIN,
      a.TERM_QRY,
      a.TERM_BIOS,
      a.TERM_IMEI,
      a.TERM_MAC,
      a.TRAD_STS_LOGIN,
      a.CR_CRD_NO,
      a.CR_DL_DT,
      a.CR_DL_STS,
      a.CR_CRD_TYP_COD,
      a.CR_CRD_STS,
      a.CR_CUST_NO,
      a.CR_CNCL_STS1,
      a.CR_CNCL_STS2,
      a.CR_CNCL_STS3,
      a.CR_CNCL_STS4,
      a.CR_CNCL_STS5,
      a.CR_CNCL_STS6,
      a.CR_COLC_FLG1,
      a.CR_COLC_FLG2,
      a.CR_COLC_FLG3,
      a.CR_COLC_FLG4,
      a.CR_COLC_FLG5,
      a.CR_COLC_FLG6,
      a.CR_COLC_FLG7,
      a.CR_COLC_FLG8,
      a.CR_ANFE_CHG_PCT,
      a.CR_ANFE_REV_AMT,
      a.CR_ANFE_YEAR,
      a.ANFE_UNPAY_MONTHS,
      a.CR_OPUN_COD,
      a.CR_MNSUB_DRP,
      a.CR_PCRD_NO,
      a.CR_CNCLC_DT,
      a.CR_CNCL_STS,
      a.CR_CHG_GRNTR_DT,
      a.CR_OPCR_DATE,
      a.CR_APP_NO,
      a.CR_CRD_CHG_DT,
      a.CR_CHG_OPUN_COD,
      a.SA_OPAC_AMT,
      a.SA_OPAC_DT,
      a.SA_CUST_NO,
      a.SA_CUST_NAME,
      a.SA_CONNTR_NO,
      a.SA_CACCT_INSTN_NO,
      a.SA_CACCT_DT,
      a.SA_OPAC_TL_NO,
      a.SA_CACCT_TL_NO,
      a.SA_YHT_SIGN_FLG,
      a.SA_XYK_FLG,
      a.SA_LMT_CTL_FLG,
      a.SA_CARD_NO,
      a.SA_FS_STS,
      a.SA_CCR_FLG,
      a.SRCSYS_CST_ID,
      a.CST_ID,
      a.CRDT_TPCD,
      a.CRDT_NO,
      a.BRTH_DT,
      a.GND_CD,
      a.NAT_CD,
      a.RSDNC_NAT_CD,
      a.PREF_LNG_CD,
      a.HSHLDRGST_ADIV_CD,
      a.MAR_STTN_CD,
      a.CHL_STTN_CD,
      a.ETHNCT_CD,
      a.RLG_CD,
      a.PLTCLPARTY_CD,
      a.LCS_CD,
      a.BLNG_INSID,
      a.CCB_EMPE_IND,
      a.PLN_FNC_EFCT_IND,
      a.IMPT_PSNG_IND,
      a.PTNL_VIP_IND,
      a.SPCLVIP_IND,
      a.STM_EVL_CST_GRD_CD,
      a.MNUL_EVL_CST_GRD_CD,
      a.PRVT_BNK_CST_GRD_CD,
      a.PRVT_BNK_SIGN_CST_IND,
      a.MO_INCMAM,
      a.CSTMGR_ID,
      a.ENTP_ADV_MGTPPL_IND,
      a.ENTP_ACT_CTRL_PSN_IND,
      a.ENLGPS_IND,
      a.NON_RSDNT_IND,
      a.WRK_UNIT_CHAR_CD,
      '2',
      b.adiv_cd
from (
   select *  
   from ${in_table}
  where p9_data_date='${log_date}'
    and ip is null
   ) a
inner join (
  select * 
    from sor.ct_t0651_ccbins_inf_h  
  where p9_end_date='29991231' 
   ) b
  on a.sa_opun_cod = b.ccbins_id
union all
    select
      a.SA_ACCT_NO,
      a.SA_DDP_ACCT_NO_DET_N,
      a.SA_OPR_NO,
      a.SA_EC_FLG,
      a.SA_EC_DET_NO,
      a.SA_CR_AMT,
      a.SA_DDP_ACCT_BAL,
      a.SA_TX_AMT,
      a.SA_TX_CRD_NO,
      a.SA_TX_TYP,
      a.SA_TX_LOG_NO,
      a.SA_DR_AMT,
      a.SA_SVC,
      a.SA_OPUN_COD,
      a.SA_DSCRP_COD,
      a.SA_RMRK,
      a.SA_TX_TM,
      a.SA_TX_DT,
      a.SA_APP_TX_CODE,
      a.SA_EXT_FLG,
      a.SA_OTX_FLG,
      a.SA_OP_ACCT_NO_32,
      a.SA_OP_CUST_NAME,
      a.SA_RMRK_ETX,
      a.SA_SPV_NO,
      a.SA_OP_BANK_NO,
      a.SA_B2B_B2C_RMRK,
      a.CR_ATM_NO,
      a.CR_TX_DT_ATM,
      a.CR_EC_FLG_ATM,
      a.CR_OPR_EC,
      a.CR_TX_NO_ATM,
      a.CR_TX_AMT_ATM,
      a.CR_TX_TM_ATM,
      a.CR_TX_NETP_NO,
      a.CR_CRD_NO_ATM,
      a.CR_DSCRP_COD,
      a.CR_TRNI_SAV_NO,
      a.CR_TRNO_SAV_NO,
      a.CR_POS_REF_NO,
      a.CR_TX_DT_POS,
      a.CR_EC_FLG_POS,
      a.CR_OPR_NO_POS,
      a.CR_TX_NO_POS,
      a.CR_TX_AMT_POS,
      a.CR_ACCT_NO_POS,
      a.CR_TX_TM_POS,
      a.CR_BRIEF,
      a.CR_TEMP_FLG,
      a.CHANL_NO,
      a.FLAT_TRAD_DATE,
      a.FLAT_TRAD_TIME,
      a.CHANL_CUST_NO,
      a.TRAD_NO,
      a.ITCD_NO,
      a.AMT1,
      a.SVC,
      a.ACCT_NO,
      a.ACCT_NAME,
      a.ASS_ACCT_NO,
      a.ASS_ACCT_TYP,
      a.ASS_ACCT_NAME,
      a.TRAD_BRAN,
      a.ACCT_BRAN,
      a.ASS_ACCT_BRAN,
      a.RETURN_CODE,
      a.RETURN_MSG,
      a.TRAD_TLR,
      a.MOBILE,
      a.IP,
      a.CUST_MGR,
      a.SIGN_BRAN,
      a.TRAD_STS,
      a.FLAT_TRAD_DATE_LOGIN,
      a.CUST_NO,
      a.FLAT_TRAD_TIME_LOGIN,
      a.RETURN_CODE_LOGIN,
      a.RETURN_MSG_LOGIN,
      a.TERM_QRY,
      a.TERM_BIOS,
      a.TERM_IMEI,
      a.TERM_MAC,
      a.TRAD_STS_LOGIN,
      a.CR_CRD_NO,
      a.CR_DL_DT,
      a.CR_DL_STS,
      a.CR_CRD_TYP_COD,
      a.CR_CRD_STS,
      a.CR_CUST_NO,
      a.CR_CNCL_STS1,
      a.CR_CNCL_STS2,
      a.CR_CNCL_STS3,
      a.CR_CNCL_STS4,
      a.CR_CNCL_STS5,
      a.CR_CNCL_STS6,
      a.CR_COLC_FLG1,
      a.CR_COLC_FLG2,
      a.CR_COLC_FLG3,
      a.CR_COLC_FLG4,
      a.CR_COLC_FLG5,
      a.CR_COLC_FLG6,
      a.CR_COLC_FLG7,
      a.CR_COLC_FLG8,
      a.CR_ANFE_CHG_PCT,
      a.CR_ANFE_REV_AMT,
      a.CR_ANFE_YEAR,
      a.ANFE_UNPAY_MONTHS,
      a.CR_OPUN_COD,
      a.CR_MNSUB_DRP,
      a.CR_PCRD_NO,
      a.CR_CNCLC_DT,
      a.CR_CNCL_STS,
      a.CR_CHG_GRNTR_DT,
      a.CR_OPCR_DATE,
      a.CR_APP_NO,
      a.CR_CRD_CHG_DT,
      a.CR_CHG_OPUN_COD,
      a.SA_OPAC_AMT,
      a.SA_OPAC_DT,
      a.SA_CUST_NO,
      a.SA_CUST_NAME,
      a.SA_CONNTR_NO,
      a.SA_CACCT_INSTN_NO,
      a.SA_CACCT_DT,
      a.SA_OPAC_TL_NO,
      a.SA_CACCT_TL_NO,
      a.SA_YHT_SIGN_FLG,
      a.SA_XYK_FLG,
      a.SA_LMT_CTL_FLG,
      a.SA_CARD_NO,
      a.SA_FS_STS,
      a.SA_CCR_FLG,
      a.SRCSYS_CST_ID,
      a.CST_ID,
      a.CRDT_TPCD,
      a.CRDT_NO,
      a.BRTH_DT,
      a.GND_CD,
      a.NAT_CD,
      a.RSDNC_NAT_CD,
      a.PREF_LNG_CD,
      a.HSHLDRGST_ADIV_CD,
      a.MAR_STTN_CD,
      a.CHL_STTN_CD,
      a.ETHNCT_CD,
      a.RLG_CD,
      a.PLTCLPARTY_CD,
      a.LCS_CD,
      a.BLNG_INSID,
      a.CCB_EMPE_IND,
      a.PLN_FNC_EFCT_IND,
      a.IMPT_PSNG_IND,
      a.PTNL_VIP_IND,
      a.SPCLVIP_IND,
      a.STM_EVL_CST_GRD_CD,
      a.MNUL_EVL_CST_GRD_CD,
      a.PRVT_BNK_CST_GRD_CD,
      a.PRVT_BNK_SIGN_CST_IND,
      a.MO_INCMAM,
      a.CSTMGR_ID,
      a.ENTP_ADV_MGTPPL_IND,
      a.ENTP_ACT_CTRL_PSN_IND,
      a.ENLGPS_IND,
      a.NON_RSDNT_IND,
      a.WRK_UNIT_CHAR_CD,
      ts150_iplocation(ip, 9),
      ts150_iplocation(ip, 4)
from (
   select *  
   from ${in_table}
  where p9_data_date='${log_date}'
    and ip is not null
   ) a;
!

}

top1000_crdt()
{

beeline <<!

use ${db};

 drop table if exists top1000_crdtno;

-- create table top1000_crdtno as
-- select distinct CRDT_NO, '351' as branch
--   from main_trade_flow_ccbs_atm_pos_ectip_card_user_a
--  where substring(SA_OPUN_COD,1,3) = '351'
--  limit 1000;

-- insert into top1000_crdtno
-- select distinct CRDT_NO, '510' as branch
--   from main_trade_flow_ccbs_atm_pos_ectip_card_user_a
--  where substring(SA_OPUN_COD,1,3) ='510'
--  limit 1000;

 create table top1000_crdtno as
 select distinct CRDT_NO, '65' as branch
   from main_trade_flow_ccbs_atm_pos_ectip_card_user_a
  where substring(CR_OPUN_COD,1,2) = '65'
  limit 1000;

 insert into top1000_crdtno
 select distinct CRDT_NO, '442' as branch
   from main_trade_flow_ccbs_atm_pos_ectip_card_user_a
  where substring(CR_OPUN_COD,1,3) = '442'
  limit 1000;
!
}

top1000_crdt_flow_no()
{

beeline <<!

use ${db};

 drop table if exists top1000_crdtno_flow_no;

 create table top1000_crdtno_flow_no as
 select --a.sa_acct_no, a.sa_ddp_acct_no_det_n,
        concat(a.SA_ACCT_NO, '_', a.SA_DDP_ACCT_NO_DET_N) as flow_no
   from main_trade_flow_ccbs_atm_pos_ectip_card_user_a a
   --left semi join top1000_crdtno b
   inner join top1000_crdtno b
    on a.crdt_no = b.CRDT_NO;

!
}
run()
{
   insert_trad_flow
}

#top1000_crdt
#top1000_crdt_flow_no