--create database train;

use train;

drop table if exists main_trade_flow_ccbs_atm_pos_ectip_card_user_a ;

create table main_trade_flow_ccbs_atm_pos_ectip_card_user_a (
    SA_ACCT_NO string,
    SA_DDP_ACCT_NO_DET_N string,
    SA_OPR_NO string,
    SA_EC_FLG string,
    SA_EC_DET_NO string,
    SA_CR_AMT string,
    SA_DDP_ACCT_BAL string,
    SA_TX_AMT string,
    SA_TX_CRD_NO string,
    SA_TX_TYP string,
    SA_TX_LOG_NO string,
    SA_DR_AMT string,
    SA_SVC string,
    SA_OPUN_COD string,
    SA_DSCRP_COD string,
    SA_RMRK string,
    SA_TX_TM string,
    SA_TX_DT string,
    SA_APP_TX_CODE string,
    SA_EXT_FLG string,
    SA_OTX_FLG string,
    SA_OP_ACCT_NO_32 string,
    SA_OP_CUST_NAME string,
    SA_RMRK_ETX string,
    SA_SPV_NO string,
    SA_OP_BANK_NO string,
    SA_B2B_B2C_RMRK string,
    CR_ATM_NO string,
    CR_TX_DT_ATM string,
    CR_EC_FLG_ATM string,
    CR_OPR_EC string,
    CR_TX_NO_ATM string,
    CR_TX_AMT_ATM string,
    CR_TX_TM_ATM string,
    CR_TX_NETP_NO string,
    CR_CRD_NO_ATM string,
    CR_DSCRP_COD string,
    CR_TRNI_SAV_NO string,
    CR_TRNO_SAV_NO string,
    CR_POS_REF_NO string,
    CR_TX_DT_POS string,
    CR_EC_FLG_POS string,
    CR_OPR_NO_POS string,
    CR_TX_NO_POS string,
    CR_TX_AMT_POS string,
    CR_ACCT_NO_POS string,
    CR_TX_TM_POS string,
    CR_BRIEF string,
    CR_TEMP_FLG string,
    CHANL_NO string,       
    FLAT_TRAD_DATE string,
    FLAT_TRAD_TIME string,
    CHANL_CUST_NO string,
    TRAD_NO string,
    ITCD_NO string,
    AMT1 string,
    SVC string,
    ACCT_NO string,
    ACCT_NAME string,
    ASS_ACCT_NO string,
    ASS_ACCT_TYP string,
    ASS_ACCT_NAME string,
    TRAD_BRAN string,
    ACCT_BRAN string,
    ASS_ACCT_BRAN string,
    RETURN_CODE string,
    RETURN_MSG string,
    TRAD_TLR string,
    MOBILE string,
    IP string,
    CUST_MGR string,
    SIGN_BRAN string,
    TRAD_STS string,
    FLAT_TRAD_DATE_LOGIN string,
    CUST_NO string,
    FLAT_TRAD_TIME_LOGIN string,
    RETURN_CODE_LOGIN string,
    RETURN_MSG_LOGIN string,
    TERM_QRY string,
    TERM_BIOS string,
    TERM_IMEI string,
    TERM_MAC string,
    TRAD_STS_LOGIN string,
    CR_CRD_NO    string,
    CR_DL_DT    string,
    CR_DL_STS    string,
    CR_CRD_TYP_COD    string,
    CR_CRD_STS    string,
    CR_CUST_NO    string,
    CR_CNCL_STS1    string,
    CR_CNCL_STS2    string,
    CR_CNCL_STS3    string,
    CR_CNCL_STS4    string,
    CR_CNCL_STS5    string,
    CR_CNCL_STS6    string,
    CR_COLC_FLG1    string,
    CR_COLC_FLG2    string,
    CR_COLC_FLG3    string,
    CR_COLC_FLG4    string,
    CR_COLC_FLG5    string,
    CR_COLC_FLG6    string,
    CR_COLC_FLG7    string,
    CR_COLC_FLG8    string,
    CR_ANFE_CHG_PCT    string,
    CR_ANFE_REV_AMT    string,
    CR_ANFE_YEAR    string,
    ANFE_UNPAY_MONTHS    string,
    CR_OPUN_COD    string,
    CR_MNSUB_DRP    string,
    CR_PCRD_NO    string,
    CR_CNCLC_DT    string,
    CR_CNCL_STS    string,
    CR_CHG_GRNTR_DT    string,
    CR_OPCR_DATE    string,
    CR_APP_NO    string,
    CR_CRD_CHG_DT    string,
    CR_CHG_OPUN_COD    string,
    SA_OPAC_AMT    string,
    SA_OPAC_DT    string,
    SA_CUST_NO    string,
    SA_CUST_NAME    string,
    SA_CONNTR_NO    string,
    SA_CACCT_INSTN_NO    string,
    SA_CACCT_DT    string,
    SA_OPAC_TL_NO    string,
    SA_CACCT_TL_NO    string,
    SA_YHT_SIGN_FLG    string,
    SA_XYK_FLG    string,
    SA_LMT_CTL_FLG    string,
    SA_CARD_NO    string,
    SA_FS_STS    string,
    SA_CCR_FLG    string,
    SRCSYS_CST_ID    string,
    CST_ID    string,
    CRDT_TPCD    string,
    CRDT_NO    string,
    BRTH_DT    string,
    GND_CD    string,
    NAT_CD    string,
    RSDNC_NAT_CD    string,
    PREF_LNG_CD    string,
    HSHLDRGST_ADIV_CD    string,
    MAR_STTN_CD    string,
    CHL_STTN_CD    string,
    ETHNCT_CD    string,
    RLG_CD    string,
    PLTCLPARTY_CD    string,
    LCS_CD    string,
    BLNG_INSID    string,
    CCB_EMPE_IND    string,
    PLN_FNC_EFCT_IND    string,
    IMPT_PSNG_IND    string,
    PTNL_VIP_IND    string,
    SPCLVIP_IND    string,
    STM_EVL_CST_GRD_CD    string,
    MNUL_EVL_CST_GRD_CD    string,
    PRVT_BNK_CST_GRD_CD    string,
    PRVT_BNK_SIGN_CST_IND    string,
    MO_INCMAM    string,
    CSTMGR_ID    string,
    ENTP_ADV_MGTPPL_IND    string,
    ENTP_ACT_CTRL_PSN_IND    string,
    ENLGPS_IND    string,
    NON_RSDNT_IND    string,
    WRK_UNIT_CHAR_CD    string,
    ip_address_type  string,
    area_code  string
)
partitioned by (p9_data_date string)
stored as orc;