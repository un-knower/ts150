

--创建卡特征表

use train;

drop TABLE IF EXISTS train_feature_cur_day;

CREATE TABLE IF NOT EXISTS train_feature_cur_day
(
card_no STRING,
crdt_no STRING,
all_cur_day_trade_num decimal(16,2),
all_cur_day_trade_amt decimal(16,2),
all_cur_day_in_num decimal(16,2),
all_cur_day_in_amt decimal(16,2),
all_cur_day_out_num decimal(16,2),
all_cur_day_out_amt decimal(16,2),
all_cur_day_trn_out_num decimal(16,2),
all_cur_day_trn_out_amt decimal(16,2),
all_cur_day_500_out_num decimal(16,2),
all_cur_day_500_out_amt decimal(16,2),
all_cur_day_10000_out_num decimal(16,2),
all_cur_day_10000_out_amt decimal(16,2),
all_cur_day_20000_out_num decimal(16,2),
all_cur_day_20000_out_amt decimal(16,2),
all_cur_day_more_out_num decimal(16,2),
all_cur_day_more_out_amt decimal(16,2),
atm_cur_day_dep_num decimal(16,2),
atm_cur_day_dep_amt decimal(16,2),
atm_cur_day_wd_num decimal(16,2),
atm_cur_day_wd_amt decimal(16,2),
atm_cur_day_trn_num decimal(16,2),
atm_cur_day_trn_amt decimal(16,2),
pos_cur_day_cnsmp_num decimal(16,2),
pos_cur_day_cnsmp_amt decimal(16,2),
nb_cur_day_out_num decimal(16,2),
nb_cur_day_out_amt decimal(16,2),
mb_cur_day_out_num decimal(16,2),
mb_cur_day_out_amt decimal(16,2)
)
partitioned by (p9_data_date STRING)
STORED AS ORC;



drop TABLE IF EXISTS train_feature_card_90_day;

CREATE TABLE IF NOT EXISTS train_feature_card_90_day
(
card_no STRING,
crdt_no STRING,
all_cur_day_trade_num decimal(16,2),
all_cur_day_trade_amt decimal(16,2),
all_cur_day_in_num decimal(16,2),
all_cur_day_in_amt decimal(16,2),
all_cur_day_out_num decimal(16,2),
all_cur_day_out_amt decimal(16,2),
all_cur_day_trn_out_num decimal(16,2),
all_cur_day_trn_out_amt decimal(16,2),
all_cur_day_500_out_num decimal(16,2),
all_cur_day_500_out_amt decimal(16,2),
all_cur_day_10000_out_num decimal(16,2),
all_cur_day_10000_out_amt decimal(16,2),
all_cur_day_20000_out_num decimal(16,2),
all_cur_day_20000_out_amt decimal(16,2),
all_cur_day_more_out_num decimal(16,2),
all_cur_day_more_out_amt decimal(16,2),
atm_cur_day_dep_num decimal(16,2),
atm_cur_day_dep_amt decimal(16,2),
atm_cur_day_wd_num decimal(16,2),
atm_cur_day_wd_amt decimal(16,2),
atm_cur_day_trn_num decimal(16,2),
atm_cur_day_trn_amt decimal(16,2),
pos_cur_day_cnsmp_num decimal(16,2),
pos_cur_day_cnsmp_amt decimal(16,2),
nb_cur_day_out_num decimal(16,2),
nb_cur_day_out_amt decimal(16,2),
mb_cur_day_out_num decimal(16,2),
mb_cur_day_out_amt decimal(16,2),
all_7_day_trade_num decimal(16,2),
all_7_day_trade_amt decimal(16,2),
all_7_day_in_num decimal(16,2),
all_7_day_in_amt decimal(16,2),
all_7_day_out_num decimal(16,2),
all_7_day_out_amt decimal(16,2),
all_7_day_trn_out_num decimal(16,2),
all_7_day_trn_out_amt decimal(16,2),
all_7_day_500_out_num decimal(16,2),
all_7_day_500_out_amt decimal(16,2),
all_7_day_10000_out_num decimal(16,2),
all_7_day_10000_out_amt decimal(16,2),
all_7_day_20000_out_num decimal(16,2),
all_7_day_20000_out_amt decimal(16,2),
all_7_day_more_out_num decimal(16,2),
all_7_day_more_out_amt decimal(16,2),
atm_7_day_dep_num decimal(16,2),
atm_7_day_dep_amt decimal(16,2),
atm_7_day_wd_num decimal(16,2),
atm_7_day_wd_amt decimal(16,2),
atm_7_day_trn_num decimal(16,2),
atm_7_day_trn_amt decimal(16,2),
pos_7_day_cnsmp_num decimal(16,2),
pos_7_day_cnsmp_amt decimal(16,2),
nb_7_day_out_num decimal(16,2),
nb_7_day_out_amt decimal(16,2),
mb_7_day_out_num decimal(16,2),
mb_7_day_out_amt decimal(16,2),
all_30_day_trade_num decimal(16,2),
all_30_day_trade_amt decimal(16,2),
all_30_day_in_num decimal(16,2),
all_30_day_in_amt decimal(16,2),
all_30_day_out_num decimal(16,2),
all_30_day_out_amt decimal(16,2),
all_30_day_trn_out_num decimal(16,2),
all_30_day_trn_out_amt decimal(16,2),
all_30_day_500_out_num decimal(16,2),
all_30_day_500_out_amt decimal(16,2),
all_30_day_10000_out_num decimal(16,2),
all_30_day_10000_out_amt decimal(16,2),
all_30_day_20000_out_num decimal(16,2),
all_30_day_20000_out_amt decimal(16,2),
all_30_day_more_out_num decimal(16,2),
all_30_day_more_out_amt decimal(16,2),
atm_30_day_dep_num decimal(16,2),
atm_30_day_dep_amt decimal(16,2),
atm_30_day_wd_num decimal(16,2),
atm_30_day_wd_amt decimal(16,2),
atm_30_day_trn_num decimal(16,2),
atm_30_day_trn_amt decimal(16,2),
pos_30_day_cnsmp_num decimal(16,2),
pos_30_day_cnsmp_amt decimal(16,2),
nb_30_day_out_num decimal(16,2),
nb_30_day_out_amt decimal(16,2),
mb_30_day_out_num decimal(16,2),
mb_30_day_out_amt decimal(16,2),
all_90_day_trade_num decimal(16,2),
all_90_day_trade_amt decimal(16,2),
all_90_day_in_num decimal(16,2),
all_90_day_in_amt decimal(16,2),
all_90_day_out_num decimal(16,2),
all_90_day_out_amt decimal(16,2),
all_90_day_trn_out_num decimal(16,2),
all_90_day_trn_out_amt decimal(16,2),
all_90_day_500_out_num decimal(16,2),
all_90_day_500_out_amt decimal(16,2),
all_90_day_10000_out_num decimal(16,2),
all_90_day_10000_out_amt decimal(16,2),
all_90_day_20000_out_num decimal(16,2),
all_90_day_20000_out_amt decimal(16,2),
all_90_day_more_out_num decimal(16,2),
all_90_day_more_out_amt decimal(16,2),
atm_90_day_dep_num decimal(16,2),
atm_90_day_dep_amt decimal(16,2),
atm_90_day_wd_num decimal(16,2),
atm_90_day_wd_amt decimal(16,2),
atm_90_day_trn_num decimal(16,2),
atm_90_day_trn_amt decimal(16,2),
pos_90_day_cnsmp_num decimal(16,2),
pos_90_day_cnsmp_amt decimal(16,2),
nb_90_day_out_num decimal(16,2),
nb_90_day_out_amt decimal(16,2),
mb_90_day_out_num decimal(16,2),
mb_90_day_out_amt decimal(16,2)
)
partitioned by (p9_data_date STRING)
STORED AS ORC;


drop TABLE IF EXISTS train_feature_crdt_90_day;

CREATE TABLE IF NOT EXISTS train_feature_crdt_90_day
(
crdt_no STRING,
all_cur_day_trade_num decimal(16,2),
all_cur_day_trade_amt decimal(16,2),
all_cur_day_in_num decimal(16,2),
all_cur_day_in_amt decimal(16,2),
all_cur_day_out_num decimal(16,2),
all_cur_day_out_amt decimal(16,2),
all_cur_day_trn_out_num decimal(16,2),
all_cur_day_trn_out_amt decimal(16,2),
all_cur_day_500_out_num decimal(16,2),
all_cur_day_500_out_amt decimal(16,2),
all_cur_day_10000_out_num decimal(16,2),
all_cur_day_10000_out_amt decimal(16,2),
all_cur_day_20000_out_num decimal(16,2),
all_cur_day_20000_out_amt decimal(16,2),
all_cur_day_more_out_num decimal(16,2),
all_cur_day_more_out_amt decimal(16,2),
atm_cur_day_dep_num decimal(16,2),
atm_cur_day_dep_amt decimal(16,2),
atm_cur_day_wd_num decimal(16,2),
atm_cur_day_wd_amt decimal(16,2),
atm_cur_day_trn_num decimal(16,2),
atm_cur_day_trn_amt decimal(16,2),
pos_cur_day_cnsmp_num decimal(16,2),
pos_cur_day_cnsmp_amt decimal(16,2),
nb_cur_day_out_num decimal(16,2),
nb_cur_day_out_amt decimal(16,2),
mb_cur_day_out_num decimal(16,2),
mb_cur_day_out_amt decimal(16,2),
all_7_day_trade_num decimal(16,2),
all_7_day_trade_amt decimal(16,2),
all_7_day_in_num decimal(16,2),
all_7_day_in_amt decimal(16,2),
all_7_day_out_num decimal(16,2),
all_7_day_out_amt decimal(16,2),
all_7_day_trn_out_num decimal(16,2),
all_7_day_trn_out_amt decimal(16,2),
all_7_day_500_out_num decimal(16,2),
all_7_day_500_out_amt decimal(16,2),
all_7_day_10000_out_num decimal(16,2),
all_7_day_10000_out_amt decimal(16,2),
all_7_day_20000_out_num decimal(16,2),
all_7_day_20000_out_amt decimal(16,2),
all_7_day_more_out_num decimal(16,2),
all_7_day_more_out_amt decimal(16,2),
atm_7_day_dep_num decimal(16,2),
atm_7_day_dep_amt decimal(16,2),
atm_7_day_wd_num decimal(16,2),
atm_7_day_wd_amt decimal(16,2),
atm_7_day_trn_num decimal(16,2),
atm_7_day_trn_amt decimal(16,2),
pos_7_day_cnsmp_num decimal(16,2),
pos_7_day_cnsmp_amt decimal(16,2),
nb_7_day_out_num decimal(16,2),
nb_7_day_out_amt decimal(16,2),
mb_7_day_out_num decimal(16,2),
mb_7_day_out_amt decimal(16,2),
all_30_day_trade_num decimal(16,2),
all_30_day_trade_amt decimal(16,2),
all_30_day_in_num decimal(16,2),
all_30_day_in_amt decimal(16,2),
all_30_day_out_num decimal(16,2),
all_30_day_out_amt decimal(16,2),
all_30_day_trn_out_num decimal(16,2),
all_30_day_trn_out_amt decimal(16,2),
all_30_day_500_out_num decimal(16,2),
all_30_day_500_out_amt decimal(16,2),
all_30_day_10000_out_num decimal(16,2),
all_30_day_10000_out_amt decimal(16,2),
all_30_day_20000_out_num decimal(16,2),
all_30_day_20000_out_amt decimal(16,2),
all_30_day_more_out_num decimal(16,2),
all_30_day_more_out_amt decimal(16,2),
atm_30_day_dep_num decimal(16,2),
atm_30_day_dep_amt decimal(16,2),
atm_30_day_wd_num decimal(16,2),
atm_30_day_wd_amt decimal(16,2),
atm_30_day_trn_num decimal(16,2),
atm_30_day_trn_amt decimal(16,2),
pos_30_day_cnsmp_num decimal(16,2),
pos_30_day_cnsmp_amt decimal(16,2),
nb_30_day_out_num decimal(16,2),
nb_30_day_out_amt decimal(16,2),
mb_30_day_out_num decimal(16,2),
mb_30_day_out_amt decimal(16,2),
all_90_day_trade_num decimal(16,2),
all_90_day_trade_amt decimal(16,2),
all_90_day_in_num decimal(16,2),
all_90_day_in_amt decimal(16,2),
all_90_day_out_num decimal(16,2),
all_90_day_out_amt decimal(16,2),
all_90_day_trn_out_num decimal(16,2),
all_90_day_trn_out_amt decimal(16,2),
all_90_day_500_out_num decimal(16,2),
all_90_day_500_out_amt decimal(16,2),
all_90_day_10000_out_num decimal(16,2),
all_90_day_10000_out_amt decimal(16,2),
all_90_day_20000_out_num decimal(16,2),
all_90_day_20000_out_amt decimal(16,2),
all_90_day_more_out_num decimal(16,2),
all_90_day_more_out_amt decimal(16,2),
atm_90_day_dep_num decimal(16,2),
atm_90_day_dep_amt decimal(16,2),
atm_90_day_wd_num decimal(16,2),
atm_90_day_wd_amt decimal(16,2),
atm_90_day_trn_num decimal(16,2),
atm_90_day_trn_amt decimal(16,2),
pos_90_day_cnsmp_num decimal(16,2),
pos_90_day_cnsmp_amt decimal(16,2),
nb_90_day_out_num decimal(16,2),
nb_90_day_out_amt decimal(16,2),
mb_90_day_out_num decimal(16,2),
mb_90_day_out_amt decimal(16,2)
)
partitioned by (p9_data_date STRING)
STORED AS ORC;


drop table if exists train_feature_trad_flow_first_a;

create table if not exists train_feature_trad_flow_first_a(
    CARD_NO string,
    CRDT_NO string,
    SA_ACCT_NO string,
    SA_DDP_ACCT_NO_DET_N string,
    CHANL_TYPE string,

    SA_APP_TX_CODE string,
    SA_TX_AMT string,
    SA_DDP_ACCT_BAL string,
    SA_DSCRP_COD string,
    SA_SVC string,
    SA_EC_FLG string,

    tx_last_time string,
    JD_FLAG string,
    IS_ZZ string,
    IS_CARD_JYLX_FIRST string,
    IS_CARD_SB_FIRST string,
    IS_CARD_ZZ_FIRST string,
    IS_CARD_JG_FIRST string,
    IS_CARD_IP_FIRST string,
    IS_CARD_AREA_FIRST string,
    IS_KH string,
    IS_BR string,
    IS_PASS_JYLX_FIRST string,
    IS_PASS_SB_FIRST string,
    IS_PASS_ZZ_FIRST string,
    IS_PASS_JG_FIRST string,
    IS_PASS_IP_FIRST string,
    IS_PASS_AREA_FIRST string,

    ASS_ACCT_DR_TIME string
)
partitioned by (p9_data_date STRING)
STORED AS ORC;

drop table if exists train_feature_trad_flow_a;

create table if not exists train_feature_trad_flow_a(
    SA_ACCT_NO string,
    SA_DDP_ACCT_NO_DET_N string,
    CHANL_TYPE string,

    SA_APP_TX_CODE string,
    SA_TX_AMT decimal(16,2),
    SA_DDP_ACCT_BAL decimal(16,2),
    SA_DSCRP_COD string,
    SA_SVC string,
    SA_EC_FLG string,

    tx_last_time string,
    JD_FLAG string,
    IS_ZZ string,
    IS_CARD_JYLX_FIRST string,
    IS_CARD_SB_FIRST string,
    IS_CARD_ZZ_FIRST string,
    IS_CARD_JG_FIRST string,
    IS_CARD_IP_FIRST string,
    IS_CARD_AREA_FIRST string,
    IS_KH string,
    IS_BR string,
    IS_PASS_JYLX_FIRST string,
    IS_PASS_SB_FIRST string,
    IS_PASS_ZZ_FIRST string,
    IS_PASS_JG_FIRST string,
    IS_PASS_IP_FIRST string,
    IS_PASS_AREA_FIRST string,

    ASS_ACCT_DR_TIME string,

    card_all_cur_day_trade_num decimal(16,2),
    card_all_cur_day_trade_amt decimal(16,2),
    card_all_cur_day_in_num decimal(16,2),
    card_all_cur_day_in_amt decimal(16,2),
    card_all_cur_day_out_num decimal(16,2),
    card_all_cur_day_out_amt decimal(16,2),
    card_all_cur_day_trn_out_num decimal(16,2),
    card_all_cur_day_trn_out_amt decimal(16,2),
    card_all_cur_day_500_out_num decimal(16,2),
    card_all_cur_day_500_out_amt decimal(16,2),
    card_all_cur_day_10000_out_num decimal(16,2),
    card_all_cur_day_10000_out_amt decimal(16,2),
    card_all_cur_day_20000_out_num decimal(16,2),
    card_all_cur_day_20000_out_amt decimal(16,2),
    card_all_cur_day_more_out_num decimal(16,2),
    card_all_cur_day_more_out_amt decimal(16,2),
    card_atm_cur_day_dep_num decimal(16,2),
    card_atm_cur_day_dep_amt decimal(16,2),
    card_atm_cur_day_wd_num decimal(16,2),
    card_atm_cur_day_wd_amt decimal(16,2),
    card_atm_cur_day_trn_num decimal(16,2),
    card_atm_cur_day_trn_amt decimal(16,2),
    card_pos_cur_day_cnsmp_num decimal(16,2),
    card_pos_cur_day_cnsmp_amt decimal(16,2),
    card_nb_cur_day_out_num decimal(16,2),
    card_nb_cur_day_out_amt decimal(16,2),
    card_mb_cur_day_out_num decimal(16,2),
    card_mb_cur_day_out_amt decimal(16,2),
    card_all_7_day_trade_num decimal(16,2),
    card_all_7_day_trade_amt decimal(16,2),
    card_all_7_day_in_num decimal(16,2),
    card_all_7_day_in_amt decimal(16,2),
    card_all_7_day_out_num decimal(16,2),
    card_all_7_day_out_amt decimal(16,2),
    card_all_7_day_trn_out_num decimal(16,2),
    card_all_7_day_trn_out_amt decimal(16,2),
    card_all_7_day_500_out_num decimal(16,2),
    card_all_7_day_500_out_amt decimal(16,2),
    card_all_7_day_10000_out_num decimal(16,2),
    card_all_7_day_10000_out_amt decimal(16,2),
    card_all_7_day_20000_out_num decimal(16,2),
    card_all_7_day_20000_out_amt decimal(16,2),
    card_all_7_day_more_out_num decimal(16,2),
    card_all_7_day_more_out_amt decimal(16,2),
    card_atm_7_day_dep_num decimal(16,2),
    card_atm_7_day_dep_amt decimal(16,2),
    card_atm_7_day_wd_num decimal(16,2),
    card_atm_7_day_wd_amt decimal(16,2),
    card_atm_7_day_trn_num decimal(16,2),
    card_atm_7_day_trn_amt decimal(16,2),
    card_pos_7_day_cnsmp_num decimal(16,2),
    card_pos_7_day_cnsmp_amt decimal(16,2),
    card_nb_7_day_out_num decimal(16,2),
    card_nb_7_day_out_amt decimal(16,2),
    card_mb_7_day_out_num decimal(16,2),
    card_mb_7_day_out_amt decimal(16,2),
    card_all_30_day_trade_num decimal(16,2),
    card_all_30_day_trade_amt decimal(16,2),
    card_all_30_day_in_num decimal(16,2),
    card_all_30_day_in_amt decimal(16,2),
    card_all_30_day_out_num decimal(16,2),
    card_all_30_day_out_amt decimal(16,2),
    card_all_30_day_trn_out_num decimal(16,2),
    card_all_30_day_trn_out_amt decimal(16,2),
    card_all_30_day_500_out_num decimal(16,2),
    card_all_30_day_500_out_amt decimal(16,2),
    card_all_30_day_10000_out_num decimal(16,2),
    card_all_30_day_10000_out_amt decimal(16,2),
    card_all_30_day_20000_out_num decimal(16,2),
    card_all_30_day_20000_out_amt decimal(16,2),
    card_all_30_day_more_out_num decimal(16,2),
    card_all_30_day_more_out_amt decimal(16,2),
    card_atm_30_day_dep_num decimal(16,2),
    card_atm_30_day_dep_amt decimal(16,2),
    card_atm_30_day_wd_num decimal(16,2),
    card_atm_30_day_wd_amt decimal(16,2),
    card_atm_30_day_trn_num decimal(16,2),
    card_atm_30_day_trn_amt decimal(16,2),
    card_pos_30_day_cnsmp_num decimal(16,2),
    card_pos_30_day_cnsmp_amt decimal(16,2),
    card_nb_30_day_out_num decimal(16,2),
    card_nb_30_day_out_amt decimal(16,2),
    card_mb_30_day_out_num decimal(16,2),
    card_mb_30_day_out_amt decimal(16,2),
    card_all_90_day_trade_num decimal(16,2),
    card_all_90_day_trade_amt decimal(16,2),
    card_all_90_day_in_num decimal(16,2),
    card_all_90_day_in_amt decimal(16,2),
    card_all_90_day_out_num decimal(16,2),
    card_all_90_day_out_amt decimal(16,2),
    card_all_90_day_trn_out_num decimal(16,2),
    card_all_90_day_trn_out_amt decimal(16,2),
    card_all_90_day_500_out_num decimal(16,2),
    card_all_90_day_500_out_amt decimal(16,2),
    card_all_90_day_10000_out_num decimal(16,2),
    card_all_90_day_10000_out_amt decimal(16,2),
    card_all_90_day_20000_out_num decimal(16,2),
    card_all_90_day_20000_out_amt decimal(16,2),
    card_all_90_day_more_out_num decimal(16,2),
    card_all_90_day_more_out_amt decimal(16,2),
    card_atm_90_day_dep_num decimal(16,2),
    card_atm_90_day_dep_amt decimal(16,2),
    card_atm_90_day_wd_num decimal(16,2),
    card_atm_90_day_wd_amt decimal(16,2),
    card_atm_90_day_trn_num decimal(16,2),
    card_atm_90_day_trn_amt decimal(16,2),
    card_pos_90_day_cnsmp_num decimal(16,2),
    card_pos_90_day_cnsmp_amt decimal(16,2),
    card_nb_90_day_out_num decimal(16,2),
    card_nb_90_day_out_amt decimal(16,2),
    card_mb_90_day_out_num decimal(16,2),
    card_mb_90_day_out_amt decimal(16,2),

    pass_all_cur_day_trade_num decimal(16,2),
    pass_all_cur_day_trade_amt decimal(16,2),
    pass_all_cur_day_in_num decimal(16,2),
    pass_all_cur_day_in_amt decimal(16,2),
    pass_all_cur_day_out_num decimal(16,2),
    pass_all_cur_day_out_amt decimal(16,2),
    pass_all_cur_day_trn_out_num decimal(16,2),
    pass_all_cur_day_trn_out_amt decimal(16,2),
    pass_all_cur_day_500_out_num decimal(16,2),
    pass_all_cur_day_500_out_amt decimal(16,2),
    pass_all_cur_day_10000_out_num decimal(16,2),
    pass_all_cur_day_10000_out_amt decimal(16,2),
    pass_all_cur_day_20000_out_num decimal(16,2),
    pass_all_cur_day_20000_out_amt decimal(16,2),
    pass_all_cur_day_more_out_num decimal(16,2),
    pass_all_cur_day_more_out_amt decimal(16,2),
    pass_atm_cur_day_dep_num decimal(16,2),
    pass_atm_cur_day_dep_amt decimal(16,2),
    pass_atm_cur_day_wd_num decimal(16,2),
    pass_atm_cur_day_wd_amt decimal(16,2),
    pass_atm_cur_day_trn_num decimal(16,2),
    pass_atm_cur_day_trn_amt decimal(16,2),
    pass_pos_cur_day_cnsmp_num decimal(16,2),
    pass_pos_cur_day_cnsmp_amt decimal(16,2),
    pass_nb_cur_day_out_num decimal(16,2),
    pass_nb_cur_day_out_amt decimal(16,2),
    pass_mb_cur_day_out_num decimal(16,2),
    pass_mb_cur_day_out_amt decimal(16,2),
    pass_all_7_day_trade_num decimal(16,2),
    pass_all_7_day_trade_amt decimal(16,2),
    pass_all_7_day_in_num decimal(16,2),
    pass_all_7_day_in_amt decimal(16,2),
    pass_all_7_day_out_num decimal(16,2),
    pass_all_7_day_out_amt decimal(16,2),
    pass_all_7_day_trn_out_num decimal(16,2),
    pass_all_7_day_trn_out_amt decimal(16,2),
    pass_all_7_day_500_out_num decimal(16,2),
    pass_all_7_day_500_out_amt decimal(16,2),
    pass_all_7_day_10000_out_num decimal(16,2),
    pass_all_7_day_10000_out_amt decimal(16,2),
    pass_all_7_day_20000_out_num decimal(16,2),
    pass_all_7_day_20000_out_amt decimal(16,2),
    pass_all_7_day_more_out_num decimal(16,2),
    pass_all_7_day_more_out_amt decimal(16,2),
    pass_atm_7_day_dep_num decimal(16,2),
    pass_atm_7_day_dep_amt decimal(16,2),
    pass_atm_7_day_wd_num decimal(16,2),
    pass_atm_7_day_wd_amt decimal(16,2),
    pass_atm_7_day_trn_num decimal(16,2),
    pass_atm_7_day_trn_amt decimal(16,2),
    pass_pos_7_day_cnsmp_num decimal(16,2),
    pass_pos_7_day_cnsmp_amt decimal(16,2),
    pass_nb_7_day_out_num decimal(16,2),
    pass_nb_7_day_out_amt decimal(16,2),
    pass_mb_7_day_out_num decimal(16,2),
    pass_mb_7_day_out_amt decimal(16,2),
    pass_all_30_day_trade_num decimal(16,2),
    pass_all_30_day_trade_amt decimal(16,2),
    pass_all_30_day_in_num decimal(16,2),
    pass_all_30_day_in_amt decimal(16,2),
    pass_all_30_day_out_num decimal(16,2),
    pass_all_30_day_out_amt decimal(16,2),
    pass_all_30_day_trn_out_num decimal(16,2),
    pass_all_30_day_trn_out_amt decimal(16,2),
    pass_all_30_day_500_out_num decimal(16,2),
    pass_all_30_day_500_out_amt decimal(16,2),
    pass_all_30_day_10000_out_num decimal(16,2),
    pass_all_30_day_10000_out_amt decimal(16,2),
    pass_all_30_day_20000_out_num decimal(16,2),
    pass_all_30_day_20000_out_amt decimal(16,2),
    pass_all_30_day_more_out_num decimal(16,2),
    pass_all_30_day_more_out_amt decimal(16,2),
    pass_atm_30_day_dep_num decimal(16,2),
    pass_atm_30_day_dep_amt decimal(16,2),
    pass_atm_30_day_wd_num decimal(16,2),
    pass_atm_30_day_wd_amt decimal(16,2),
    pass_atm_30_day_trn_num decimal(16,2),
    pass_atm_30_day_trn_amt decimal(16,2),
    pass_pos_30_day_cnsmp_num decimal(16,2),
    pass_pos_30_day_cnsmp_amt decimal(16,2),
    pass_nb_30_day_out_num decimal(16,2),
    pass_nb_30_day_out_amt decimal(16,2),
    pass_mb_30_day_out_num decimal(16,2),
    pass_mb_30_day_out_amt decimal(16,2),
    pass_all_90_day_trade_num decimal(16,2),
    pass_all_90_day_trade_amt decimal(16,2),
    pass_all_90_day_in_num decimal(16,2),
    pass_all_90_day_in_amt decimal(16,2),
    pass_all_90_day_out_num decimal(16,2),
    pass_all_90_day_out_amt decimal(16,2),
    pass_all_90_day_trn_out_num decimal(16,2),
    pass_all_90_day_trn_out_amt decimal(16,2),
    pass_all_90_day_500_out_num decimal(16,2),
    pass_all_90_day_500_out_amt decimal(16,2),
    pass_all_90_day_10000_out_num decimal(16,2),
    pass_all_90_day_10000_out_amt decimal(16,2),
    pass_all_90_day_20000_out_num decimal(16,2),
    pass_all_90_day_20000_out_amt decimal(16,2),
    pass_all_90_day_more_out_num decimal(16,2),
    pass_all_90_day_more_out_amt decimal(16,2),
    pass_atm_90_day_dep_num decimal(16,2),
    pass_atm_90_day_dep_amt decimal(16,2),
    pass_atm_90_day_wd_num decimal(16,2),
    pass_atm_90_day_wd_amt decimal(16,2),
    pass_atm_90_day_trn_num decimal(16,2),
    pass_atm_90_day_trn_amt decimal(16,2),
    pass_pos_90_day_cnsmp_num decimal(16,2),
    pass_pos_90_day_cnsmp_amt decimal(16,2),
    pass_nb_90_day_out_num decimal(16,2),
    pass_nb_90_day_out_amt decimal(16,2),
    pass_mb_90_day_out_num decimal(16,2),
    pass_mb_90_day_out_amt decimal(16,2)    
)
partitioned by (p9_data_date STRING)
stored as ORC;







create table if not exists train_feature_trad_flow_first_a_tmp (
    CARD_NO string,
    CRDT_NO string,
    SA_ACCT_NO string,
    SA_DDP_ACCT_NO_DET_N string,
    CHANL_TYPE string,

    SA_APP_TX_CODE string,
    SA_TX_AMT string,
    SA_DDP_ACCT_BAL string,
    SA_DSCRP_COD string,
    SA_SVC string,
    SA_EC_FLG string,

    tx_last_time string,
    JD_FLAG string,
    IS_ZZ string,
    IS_CARD_JYLX_FIRST string,
    IS_CARD_SB_FIRST string,
    IS_CARD_ZZ_FIRST string,
    IS_CARD_JG_FIRST string,
    IS_CARD_IP_FIRST string,
    IS_CARD_AREA_FIRST string,
    IS_KH string,
    IS_BR string,
    IS_PASS_JYLX_FIRST string,
    IS_PASS_SB_FIRST string,
    IS_PASS_ZZ_FIRST string,
    IS_PASS_JG_FIRST string,
    IS_PASS_IP_FIRST string,
    IS_PASS_AREA_FIRST string,

    ASS_ACCT_DR_TIME string,

    SA_TX_DT string,
    SA_TX_TM string,
    CR_ATM_NO string, 
    CR_POS_REF_NO string,
    mobile string, 
    term_qry string,
    SA_OP_ACCT_NO_32 string,
    SA_OPUN_COD string,
    ip string,
    area_code string

)
partitioned by (p9_data_date STRING)
STORED AS ORC;


