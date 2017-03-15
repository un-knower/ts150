#!/bin/sh
# 简易Hive脚本调度程序

source /home/ap/dip/appjob/shelljob/TS150/violate/base.sh
source /home/ap/dip/appjob/shelljob/TS150/violate/7_scheduler/hadoop_check_fun.sh

# 默认运行脚本目录
base_path=/home/ap/dip/appjob/shelljob/TS150/violate/
# 默认Hive数据库名
default_db=sor

split_tb_name() {
   # 输入表名，返回库名与表名
   local tb=$1

   # 表名的表示方式，以"库名.表名"或默认"表名"
   word_num=`echo $tb | awk -F "." '{print $1,$2}' | wc -w`
   if [ $word_num -eq 1 ]; then
      out_db=$default_db
      out_tb=$tb
   fi
   if [ $word_num -eq 2 ]; then
     out_db=`echo $tb | awk -F "." '{print $1}'`
     out_tb=`echo $tb | awk -F "." '{print $2}'`
   fi
}

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
wait_hive_data()
{
   local local_date=$1
   local table_list=$2
   local partition_field=${3:-p9_data_date}
   
   # 跑批第一天的前一天准备数据不需要
   if [ "$local_date" == "$prev_start_date" ]; then
      return 0
   fi

   # 等待当天Hive分区表数据
   for tb in $table_list; do
      # 处理表名，如ts150.tab_name
      split_tb_name $tb
      for ((i=0;i<10000;i=i+1)); do
         log_l "等待Hive分区数据:" $out_db $out_tb $local_date
         hive_partition_over "$out_tb" "$local_date" "$out_db" "$partition_field"
         if [ $? -eq 0 ]; then
            log_l "到达Hive分区数据:" $out_db $out_tb $local_date
            break
         fi
         sleep 100
      done  # end for 1--10000
   done
   return 0
}

# 等待HDFS文件
wait_hdfs_data()
{
   local local_date=$1
   local hdfs_path=$2
   local file_path=${hdfs_path}${local_date}
   
   # 跑批第一天的前一天准备数据不需要
   if [ "$local_date" == "$prev_start_date" ]; then
      return 0
   fi

   # 等待当天HDFS数据
   for ((i=0;i<10000;i=i+1)); do
      log_l "等待HDFS数据:" $file_path
      hdfs_file_exists $file_path
      if [ $? -eq 0 ]; then
         log_l "到达HDFS数据:" $file_path
         break
      fi
      sleep 100
   done  # end for 1--10000

   return 0
}

# 检查Hive运行结果是否已完成
check_hive_finish()
{
   local local_date=$1
   local table_list=$2
   local partition_field=${3:-p9_data_date}

   # 判断本脚本是否已运行完成，通过判断结果表分区
   local no_finished_num=0
   for table in $table_list; do
      # 处理表名，如ts150.tab_name
      split_tb_name $table

      hive_partition_over "$out_tb" "$local_date" "$out_db" "$partition_field"
      if [ $? -eq 0 ]; then
         log_l "数据已存在：[$fun][$out_db.$out_tb][$local_date]"
      else
         # 多个输出表累加判断
         no_finished_num=`expr $no_finished_num + 1`
      fi
   done
   if [ $no_finished_num -eq 0 ]; then
      log_l "本日跑批完成，[$fun][$data_date]"
      return 0
   fi

   return 1
}

# Shell命令行参数解释
arg()
{
   local OPTIND
   unset OPTIND

   while getopts "c:s:e:d:wf" OPTION; do
      # echo "=====" $OPTION $OPTARG
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
         w) echo "wait"
           ;;
         f) echo "fouce"
           ;;
         \?) 
           ;;
      esac
   done

   # 命令行参数有效性判断
   if [ "${script:-NULL}" = "NULL" ]; then
      echo "-c script.sh must input"
      exit 1
   fi
   # 运行脚本是否存在
   full_script=$base_path/$script
   if [ ! -f $full_script ]; then
      full_script=`find $base_path -name "$script"`
      if [ "${full_script:-NULL}" = "NULL" ]; then
         echo "-c $script 运行脚本不存在！"
         exit 1
      fi
   fi
   fun="${script%%.*}"

   # 日期格式有效性
   if [ "${start_date:-NULL}" != "NULL" ]; then
      valid_date $start_date
      if [ $? -ne 0 ]; then
         echo "start_date format error"
         exit 1
      fi
      # start_date前一天无数据，不依赖
      prev_start_date=`date -d "$start_date 1 days ago" +"%Y%m%d"`
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
      # 只跑一天数据，需要等待其前一天数据
      # prev_start_date=`date -d "$data_date 1 days ago" +"%Y%m%d"`
   fi

   unset OPTIND
}

# 一天数据跑批脚本
run_one_date()
{
   # 装载跑批脚本
   echo $full_script -d $data_date
   source $full_script -d $data_date

   log_l "================================================"
   log_l "start [$fun][$data_date]..."

   # 已完成运行判断，不重复运行
   check_hive_finish $data_date $OUT_CUR_HIVE
   if [ $? -eq 0 ]; then
      return 0
   fi

   # 等待当天Hive分区表数据
   wait_hive_data $data_date $IN_CUR_HIVE

   # 等待HDFS文件
   wait_hdfs_data $data_date $IN_CUR_HDFS

   # 等待前一天Hive分区表数据
   local less_1_date=`date -d "$data_date 1 days ago" +"%Y%m%d"`
   wait_hive_data $less_1_date $IN_PRE_HIVE

   # 运行结果判断, 出错重跑
   for second in 10 30 60 300; do
      # 运行实际处理脚本
      run
      log_l "脚本执行完成[$fun]返回：[$?][$data_date][$OUT_CUR_HIVE]"
      check_hive_finish $data_date $OUT_CUR_HIVE
      if [ $? -eq 0 ]; then
         break
      fi
      sleep $second
   done
   log_l "脚本执行完成，检查结果成功，退出[$fun][$data_date][$OUT_CUR_HIVE]"
   return 0
}

# 运行从start_date到end_date所有日期的处理脚本
run_all_date()
{
   for ((data_date=$start_date; data_date<=$end_date;data_date=`date -d "$data_date 1 days" +"%Y%m%d"`)) do
      echo "开始跑批：$data_date"
      run_one_date
      # 跑批失败，中断
      if [ $? -ne 0 ]; then
         return $?
      fi
   done
}

# 主程序开始运行
#init
arg $*

# 有-d参数时，只跑一天数据，否则跑start_date到end_date间每一天数据
if [ "${data_date:-NULL}" != "NULL" ]; then
   run_one_date
   exit $?
else
   run_all_date
   exit $?
fi
