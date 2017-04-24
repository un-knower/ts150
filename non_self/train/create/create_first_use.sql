use train;

drop table if exists stat_tx_last_time_a;

create table if not exists stat_tx_last_time_a(
    card_no string,
    pass_num string,
    flag_id string,
    last_date string,
    last_time string
)
partitioned by (p9_data_date STRING, flag_type STRING)
stored as ORC;

alter table stat_tx_last_time_a add partition(p9_data_date='20151105', flag_type='1');
alter table stat_tx_last_time_a add partition(p9_data_date='20151105', flag_type='2');
alter table stat_tx_last_time_a add partition(p9_data_date='20151105', flag_type='3');
alter table stat_tx_last_time_a add partition(p9_data_date='20151105', flag_type='4');
alter table stat_tx_last_time_a add partition(p9_data_date='20151105', flag_type='5');
alter table stat_tx_last_time_a add partition(p9_data_date='20151105', flag_type='6');
alter table stat_tx_last_time_a add partition(p9_data_date='20151105', flag_type='7');
alter table stat_tx_last_time_a add partition(p9_data_date='20151105', flag_type='8');


drop table if exists stat_card_tx_last_time_h;

create table if not exists stat_card_tx_last_time_h(
    card_no string,
    flag_collect string
)
partitioned by (p9_data_date STRING)
stored as ORC;

drop table if exists stat_pass_tx_last_time_h;

create table if not exists stat_pass_tx_last_time_h(
    pass_num string,
    flag_collect string
)
partitioned by (p9_data_date STRING)
stored as ORC;
