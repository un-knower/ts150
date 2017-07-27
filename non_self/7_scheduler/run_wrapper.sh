#!/bin/sh
# 简易Hive脚本调度程序

args=$*
# 默认运行脚本目录
#base_path=/home/ap/dip/appjob/shelljob/TS150/violate
base_path=/home/ap/dip_ts150/ts150_script/ccb_risk_scoring
run_path=$base_path/train

source $base_path/base.sh
#source /home/ap/dip/appjob/shelljob/TS150/violate/base.sh
source $base_path/7_scheduler/hadoop_check_fun.sh

# 默认Hive数据库名
default_db=train

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
   touch ${run_path}/run_status.log
   chmod 777 ${run_path}/run_status.log
}

# 等待Hive分区数据
wait_hive_data()
{
   local local_date=$1
   local table_list=$2
   # local partition_field=${3:-p9_data_date}
   
   # 跑批第一天的前一天准备数据不需要
   if [ "$local_date" == "$prev_start_date" ]; then
      log "不检查跑批第一天的前一天Hive数据:[$local_date][$prev_start_date]"
      return 0
   fi

   # 等待当天Hive分区表数据
   for tb in $table_list; do
      # 处理表名，如ts150.tab_name
      split_tb_name $tb
      for ((i=0;i<10000;i=i+1)); do
         log "等待Hive分区数据:[$i][$local_date][${out_db}.${out_tb}]"
         hive_partition_over "$out_tb" "$local_date" "$out_db" "$check_file_size"
         if [ $? -eq 0 ]; then
            log "到达Hive分区数据:[$i][$local_date][${out_db}.${out_tb}]"
            break
         fi
         if [ "$no_wait" = "true" ]; then
            log_error "等待Hive数据未到达，异常退出:[$local_date][${out_db}.${out_tb}]"
            return 1
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
   
   if [ "$hdfs_path" = "" ]; then
      return 0
   fi

   # 跑批第一天的前一天准备数据不需要
   if [ "$local_date" = "$prev_start_date" ]; then
      log "不检查跑批第一天的前一天HDFS数据:[$local_date][$prev_start_date]"
      return 0
   fi

   # 等待当天HDFS数据
   for ((i=0;i<10000;i=i+1)); do
      log "等待HDFS数据: $file_path"
      hdfs_file_exists $file_path $check_file_size
      if [ $? -eq 0 ]; then
         log "到达HDFS数据: $file_path"
         break
      fi
      if [ "$no_wait" = "true" ]; then
         log_error "等待HDFS数据未到达，异常退出:[$file_path]"
         return 1
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
   
   log "检查Hive表分区是否已完成:[$local_date][$table_list]"
   # 判断本脚本是否已运行完成，通过判断结果表分区
   local no_finished_num=0
   for table in $table_list; do
      # 处理表名，如ts150.tab_name
      split_tb_name $table

      hive_partition_over "$out_tb" "$local_date" "$out_db" "$check_file_size"
      if [ $? -eq 0 ]; then
         log "Hive表分区数据已存在:[$local_date][$out_db.$out_tb]"
      else
         # 多个输出表累加判断
         no_finished_num=`expr $no_finished_num + 1`
      fi
   done
   if [ $no_finished_num -eq 0 ]; then
      log "Hive表分区已全部完成:[$local_date][$table_list]"
      return 0
   else
      log "Hive表分区分未完成:[$local_date][$table_list][$no_finished_num]"
      return 1
   fi
}

# Shell命令行参数解释
arg()
{
   local OPTIND
   unset OPTIND

   while getopts "c:s:e:d:wfnov" OPTION; do
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
         w) #echo "no wait" # 源数据未准备好时退出
           no_wait=true
           ;;
         f) #echo "force"   # 强制重跑
           force=true
           ;;
         n) #echo "error notice"  # 出错时短信邮件通知
           error_notice=true
           ;;
         o) #echo "over notice"  # 完成时短信邮件通知
           over_notice=true
           ;;
         v) #echo "via_db_check"  # 通过Task任务表检查完成情况
           via_db_check=true
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
   full_script=$run_path/$script
   if [ ! -f $full_script ]; then
      #full_script=`find $run_path -name "$script"`
      full_script=`find /home/ap/dip_ts150/ts150_script/ccb_risk_scoring/train -name "$script"`
      echo $full_script
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
   log "启动调用程序:${fun}"

   unset OPTIND
}

# 一天数据跑批脚本
run_one_date()
{
   # 装载跑批脚本
   # echo $full_script -d $data_date
   source $full_script -d $data_date

   log "开始运行脚本: [$full_script][$fun][$data_date]"

   # 已完成运行判断，不重复运行
   if [ "$force" != "true" ]; then
      if [ "$via_db_check" = "true" ]; then
         log "通过Task任务表检查完成情况:[$fun][$data_date]"
         task_query $fun $data_date
      else
         check_hive_finish $data_date $OUT_CUR_HIVE
      fi
      if [ $? -eq 0 ]; then
         return 0
      fi
   fi

   # 等待当天Hive分区表数据
   wait_hive_data $data_date "$IN_CUR_HIVE" || return 1

   # 等待HDFS文件
   wait_hdfs_data $data_date "$IN_CUR_HDFS" || return 2

   # 等待前一天Hive分区表数据
   local less_1_date=`date -d "$data_date 1 days ago" +"%Y%m%d"`
   wait_hive_data $less_1_date "$IN_PRE_HIVE" || return 3

   # 运行结果判断, 出错重跑
   success=false
   for second in 10 30 60 300; do
      # 运行实际处理脚本
      run
      log "脚本执行完成[$fun][$data_date][$OUT_CUR_HIVE] 返回:[$?]"
      check_hive_finish $data_date $OUT_CUR_HIVE
      if [ $? -eq 0 ]; then
         success=true
         break
      fi
      log "脚本执行完成[$fun]检查结果异常:[$data_date][$OUT_CUR_HIVE],等待[$second]秒后重跑"
      sleep $second
   done

   if [ "$success" = "true" ]; then
      log "脚本执行完成，检查结果成功，退出[$fun][$data_date][$OUT_CUR_HIVE]"
      task_insert $fun $data_date 0
      return 0
   else
      log_error "脚本执行结果，检查结果出错，退出[$fun][$data_date][$OUT_CUR_HIVE]"
      task_insert $fun $data_date 1
      return 4
   fi
}

# 判断两个字符串是否有交集
has_intersection()
{
   str1=$1
   str2=$2
   for s1 in $str1; do
      for s2 in $str2; do
         if [ "$s1" = "$s2" ]; then
            return 0
         fi
      done
   done
   return 1
}

# 运行从start_date到end_date所有日期的处理脚本
run_all_date()
{
   for ((data_date=$start_date;data_date<=$end_date;data_date=`date -d "$data_date 1 days" +"%Y%m%d"`)) do
      # echo "开始跑批:$data_date"
      run_one_date
      ret=$?
      if [ $ret -ne 0 ]; then
         # 输出表被当成下一天的输入表，则不能继续跑下一下
         has_intersection $OUT_CUR_HIVE $IN_PRE_HIVE
         if [ $? -eq 0 ]; then
            # 有交集，跑批失败，中断
            return 1
         else
            $base_path/7_scheduler/sendMail.sh "$0 $args运行出错退出$ret,$last_error, 判断可以跳过跑下一天" "调度脚本运行出错"
            log_error "脚本运行出错[$data_date], 判断可以跳过跑下一天" $ret
         fi
      fi
   done
}

# 主程序开始运行
#init
arg $*

# 有-d参数时，只跑一天数据，否则跑start_date到end_date间每一天数据
if [ "${data_date:-NULL}" != "NULL" ]; then
   run_one_date
   ret=$?
else
   run_all_date
   ret=$?
fi

if [ "$error_notice" = "true" -a $ret -ne 0 ]; then
   $base_path/7_scheduler/sendMail.sh "$0 $args运行出错退出$ret,$last_error" "调度脚本运行出错"
   echo "$0 $args运行出错退出:$ret,$last_error" "调度脚本运行出错"
   log_error "脚本出错退出，发送通知信息" $ret
fi
if [ "$over_notice" = "true" -a $ret -eq 0 ]; then
   $base_path/7_scheduler/sendMail.sh "$0 $args运行成功完成" "调度脚本运行成功"
   echo "$0 $args运行成功完成" "调度脚本运行成功"
   log "脚本运行完成退出，发送通知信息" $ret
fi

exit $ret
