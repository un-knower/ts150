#!/bin/sh
# 简易Hive脚本调度程序
#

source /home/ap/dip_ts150/ts150_script/monitor/base.sh

# 默认运行脚本目录
base_path=/home/ap/dip_ts150/ts150_script/violate/7_scheduler
# 默认Hive数据库名
db=sor

init()
{
   touch ${base_path}/run_status.log
   chmod 777 ${base_path}/run_status.log
}

log_l()
{
   local logTime=`date "+%Y-%m-%d %H:%M:%S"`
   local msg="$1 $2 $3$4$5$6"
   local execute=`basename $0`

   echo $msg

   echo "$logTime $execute $$ $msg" >> ${base_path}/run_status.log
   echo "$logTime $execute $msg" >> ${base_path}/log/run_status.$$
}

# 等待Hive分区数据
wait_data()
{
   local data_date=$1
   local table=$2
   local database=${3:-verify_2}
   local partition_field=${4:-p9_data_date}
   
   if [ $data_date == $first_date ]; then
      return 0
   fi

   for ((i=0;i<10000;i=i+1)); do
      log_l "wait data" $table $data_date
      hive_partition_over $table ${data_date} $database $partition_field
      if [ $? -eq 0 ]; then
         log_l "wait data reach" $table $data_date
         break
      fi
      sleep 100
   done

   return 0
}

# 运行从start_date到end_date所有日期的处理脚本
run_all_date()
{
   for ((data_date=$start_date; data_date<=$end_date;data_date=`date -d "$data_date 1 days" +"%Y%m%d"`)) do
      echo $data_date
      run_one_date
   done
}

# Shell命令行参数解释
arg()
{
   local OPTIND
   unset OPTIND

   while getopts "c:s:e:d:" OPTION
   do
      echo "=====" $OPTION $OPTARG
      case $OPTION in
         c)script=$OPTARG
           ;;
         s)start_date=$OPTARG
           ;;
         e)end_date=$OPTARG
           ;;
         d)data_date=$OPTARG
           OPT=yes
           ;;
         \?) 
           ;;
      esac
      #run $OPTION
   done

   # 命令行参数有效性判断
   if [ "${script:-NULL}" = "NULL" ]; then
      echo "-c script.sh must input"
      exit 1
   fi

   if [ ! -f $base_path/$script ]; then
      echo "$base_path/$script not found"
      exit 1
   fi
   fun="${script%%.*}"

   if [ "${start_date:-NULL}" != "NULL" ]; then
      valid_date $start_date
      if [ $? -ne 0 ]; then
         echo "start_date format error"
         exit 1
      fi
      first_date=`date -d "$start_date 1 days ago" +"%Y%m%d"`
   fi

   if [ "${end_date:-NULL}" != "NULL" ]; then
      valid_date $end_date
      if [ $? -ne 0 ]; then
         echo "end_date format error"
         exit 1
      fi
   fi

   if [ "${data_date:-NULL}" != "NULL" ]; then
      valid_date $data_date
      if [ $? -ne 0 ]; then
         echo "data_date format error"
         exit 1
      fi
      first_date=`date -d "$data_date 1 days ago" +"%Y%m%d"`
   fi

   unset OPTIND
}


# 一天数据跑批脚本
run_one_date()
{
   echo "calling..."
   echo $base_path/$script -d $data_date
   source $base_path/$script -d $data_date
   local less_1_date=`date -d "$data_date 1 days ago" +"%Y%m%d"`

   log_l "================================================"
   log_l "start [$fun][$data_date]..."

   # 判断本脚本是否已运行完成，通过判断结果表分区
   local no_finished_num=0
   for table in $OUT_CUR_HIVE; do
      hive_partition_over $table ${data_date} $db
      if [ $? -eq 0 ]; then
         log_l "[$fun][$db.$table][$data_date] already finished."
      else
         # 多个输出表累加判断
         no_finished_num=`expr $no_finished_num + 1`
      fi
   done
   if [ $no_finished_num -eq 0 ]; then
      log_l "[$fun][$data_date] already finished. exit..."
      return 0
   fi

   # 等待当天Hive分区表数据
   for tb in $IN_CUR_HIVE; do
      # 表名的表示方式，以"库名.表名"或默认"表名"
      word_num=`echo $tb | awk -F "." '{print $1,$2}' | wc -w`
      if [ $word_num -eq 1 ]; then
         in_db=$db
         in_tb=$tb
      fi
      if [ $word_num -eq 2 ]; then
        in_db=`echo $tb | awk -F "." '{print $1}'`
        in_tb=`echo $tb | awk -F "." '{print $2}'`
      fi
      echo "wait data $in_db $in_tb $data_date"
      wait_data $data_date $in_tb $in_db
   done

   # 等待前一天Hive分区表数据
   for tb in $IN_PRE_HIVE; do
      word_num=`echo $tb | awk -F "." '{print $1,$2}' | wc -w`
      if [ $word_num -eq 1 ]; then
         in_db=$db
         in_tb=$tb
      fi
      if [ $word_num -eq 2 ]; then
        in_db=`echo $tb | awk -F "." '{print $1}'`
        in_tb=`echo $tb | awk -F "." '{print $2}'`
      fi
      echo "wait data $in_db $in_tb $less_1_date"
      wait_data $less_1_date $in_tb $in_db
   done

   # 运行结果判断
   for second in 10 30 60 300; do
      run
      log_l "run over [$fun][$data_date][$OUT_CUR_HIVE]"

      local no_finished_num=0
      for table in $OUT_CUR_HIVE; do
         hive_partition_over $table ${data_date} $db
         if [ $? -eq 0 ]; then
            log_l "[$fun][$db.$table][$data_date] finished."
         else
            no_finished_num=`expr $no_finished_num + 1`
         fi
      done
      if [ $no_finished_num -eq 0 ]; then
         log_l "[$fun][$data_date] finished." 
         break
      fi

      sleep $second
   done
   log_l "end [$fun][$data_date][$OUT_CUR_HIVE]"
}

#init
arg $*

# 有-d参数时，只跑一天数据，否则跑start_date到end_date间每一天数据
if [ "${data_date:-NULL}" != "NULL" ]; then
   run_one_date
else
   run_all_date
fi
