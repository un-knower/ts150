
cmd_list1="
  1_insert_main_flow_user.sh
  2_export_feature_cur_day_train.sh  
  3_export_feature_90_day_train.sh
  3_2_export_feature_90_day_train.sh
  4_insert_first_use_train.sh
  4_2_insert_first_use_train.sh
  5_train_feature_trad_flow_first_a.sh
  6_export_feature_over.sh
"

cmd_list="
  4_insert_first_use_train.sh
  4_2_insert_first_use_train.sh
   "

run_wrapper=/home/ap/dip_ts150/ts150_script/ccb_risk_scoring/7_scheduler/run_wrapper.sh

for cmd in $cmd_list; do
   logfile="./log/${cmd%%.*}.log"
   nohup $run_wrapper -c $cmd -s 20160717 -e 20161206 -v > $logfile  &
   #$run_wrapper -c $cmd -s 20160511 -e 20160801 
   #$run_wrapper -c $cmd -d 20151125
done

