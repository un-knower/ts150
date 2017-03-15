#!/bin/sh

########################################
#   案件溯源基础函数库与常用变量
#        wuzhaohui@tienon.com  2017.3
#------------------常用变量-----------------
# Hadoop环境（实验、归档）
HADOOP_ENV=hds
# HADOOP_ENV=ts150

#Hive UDF Jar包存放目录
udfjarpath=hdfs:///bigdata/common/function

script_path=/home/ap/dip/appjob/shelljob/TS150/violate
# 脚本运行目录
if [ "$0" = "-bash" ]; then
   running_path=$(cd `dirname $BASH_SOURCE`; pwd)
else
   running_path=$(cd `dirname $0`; pwd)
fi

#默认的Hadoop用户名
hadoop_user=TS150

#Hadoop运行组件
hdsrun=/home/ap/dip/hadoop/bdap/bin/hdsrun

#
aqpath=/home/ap/dip/appjob/shelljob/TS150/hadoop_aq

#hds数据储存目录
hdsinput=/home/ap/dip/file/archive/input/ts150/000000000/data

#hadoop目录,案件溯源数据输入目录
hdfsoutput=/bigdata/output/TS150
#hdfsoutput=/bigdata/output/MM_XK_USER1/case_trace
#hdfsinput=/bigdata/input/TS150/case_trace
hdfsinput=/bigdata/input/TS150


curDate=`date +%Y%m%d`
curTime=`date "+%Y-%m-%d_%H:%M:%S"`

less_1_date=`date -d "$curDate 1 days ago" +"%Y%m%d"`
less_2_date=`date -d "$curDate 2 days ago" +"%Y%m%d"`
less_3_date=`date -d "$curDate 3 days ago" +"%Y%m%d"`
less_7_date=`date -d "$curDate 7 days ago" +"%Y%m%d"`
less_30_date=`date -d "$curDate 30 days ago" +"%Y%m%d"`
less_90_date=`date -d "$curDate 90 days ago" +"%Y%m%d"`

logpath=$HOME/log/$curDate/ts150               #日志目录
if [ ! -d $logpath ]; then
   mkdir -p $logpath
fi

usage()
{
   echo "命令行参数有误"
   echo "`basename $0` -d yyyymmdd [-u hadoop_user]"
   exit 1
}

#日期参数检验
valid_date()
{
   local date=$1
   if [ `echo $date | egrep "^2[0-9]{3}[0-1][0-9][0-3][0-9]$"` ]; then
      return 0
   else
      return 1
   fi
}

valid()
{
   if [ `echo $log_date | egrep "^2[0-9]{3}[0-1][0-9][0-3][0-9]$"` ]
   then
      echo "date OK"
   else
      echo "date error"
      usage
   fi
}

#日期参数检验
valid_gp()
{
   if [ `echo $log_date | egrep "^2[0-9]{3}[0-1][0-9][0-3][0-9]$"` ]
   then
      echo "date OK"
   else
      echo "date error"
      usage_gp
   fi
}

copyoutTable()
{
   table=$1
   #hdfsBaseDir=/bigdata/output/$hadoop_user/ts150
   localDir=$HOME/file/output/ts150/000000000/data/$log_date
   localFilePath=$localDir/${table}_${log_date}
   localFileName=$localDir/${table}_${log_date}.dat

   $hdsrun hdfsCopyFilelocal USERNAME:$hadoop_user,INSTANCEID:A00013-${log_date}-0000 DATA_DATE:${log_date},INPUT:ts150/$table/$log_date/,OUTPATH:${localFileName},ISREMOVE:false

   chmod a+rw ${localFileName}
   
   #cat $localFilePath/* > $localFileName
   #rm -rf $localFilePath
}

#日志记录到文件
log()
{
   return_code=$?

   #echo $return_code
   #echo $0	
   if [ $return_code -eq 0 ]; then
      return_msg="执行成功"
   else
      return_msg="执行失败，返回值:$return_code"
   fi

   logTime=`date "+%Y-%m-%d %H:%M:%S"`
   #logTime=`date "+%H:%M:%S"`
   msg="$1 $2 $3$4$5$6"
   execute=`basename $0`
   
   #echo $curDate
   #echo $logTime
   echo $msg
   #echo $execute
  
   echo "$msg" >> $logpath/case_trace.$curDate
   echo "$logTime $execute PID:$$ $return_msg" >> $logpath/case_trace.$curDate
   #echo "$msg" >> $logpath/case_trace.$curDate
   #echo "$logTime $execute $msg" >> $logpath/case_trace.$curDate

}
log_exit()
{
   log $*
   if [ $return_code -ne 0 ]; then
      exit $return_code
   fi
}

logfile()
{
   file=$1

   echo "显示日志文件：$file" >> $logpath/case_trace.$curDate

   cat $file >> $logpath/case_trace.$curDate
}
log_sms()
{
   #logTime=`date "+%Y-%m-%d %H:%M:%S"`
   logTime=`date "+%H:%M:%S"`
   msg="$1 $2 $3 $4$5$6"
   execute=`basename $0`
   
   #echo $curDate
   #echo $logTime
   echo $msg
   #echo $execute
  
   echo "$msg" >> $logpath/case_trace.$curDate
   echo "$logTime $execute PID:$$ $return_msg" >> $logpath/case_trace.$curDate
   #echo "$msg" >> $logpath/case_trace.$curDate
   #echo "$logTime $execute $msg" >> $logpath/case_trace.$curDate

}
#获取详细日志信息
getLog_file(){
  logFile=$1
  startLogTime=$2
  endLogTime=$3
  msg=sed -n '/$startLogTime/=' $logpath/case_trace.$curDate
  echo "$msg"

}
#输入命令行参数解释，使用该函数时传入参数$*
logdate_arg()
{
   unset OPTIND
   OPT=no
   while getopts :u:d: OPTION
   do
      case $OPTION in
         u)hadoop_user=$OPTARG
           ;;
         d)log_date=$OPTARG
           OPT=yes
           ;;
         \?) usage
           ;;
      esac
   done

   if [ "$OPT" = "no" ]
   then
      usage
   fi

   valid

   #log "============================================================================"
   #log "运行脚本：$0 业务日期：$log_date  Hadoop用户：$hadoop_user"
   
   log_info "============================================================================"
   log_info "运行脚本：$0 业务日期：$log_date  Hadoop用户：$hadoop_user"
}


usage_hive()
{
   echo "命令行参数有误"
   echo "`basename $0` -u hadoop_user -d yyyymmdd [-n delta_t]"
   exit 1
}

#输入命令行参数解释，使用该函数时传入参数$*
logdate_hive()
{
   delta_t=30
   OPT=no
   while getopts :u:d:n: OPTION
   do
      case $OPTION in
         u)hadoop_user=$OPTARG
           ;;
         d)log_date=$OPTARG
           OPT=yes
           ;;
         n)delta_t=$OPTARG
           ;;
         \?) usage_hive
           ;;
      esac
   done

   if [ "$OPT" = "no" ]
   then
      usage_hive
   fi

   valid
   log_info "============================================================================"
   log_info "运行脚本：$0 业务日期：$log_date  Hadoop用户：$hadoop_user"
}


datetable_arg()
{
   unset OPTIND
   OPT=no
   while getopts :u:d:t: OPTION
   do
      case $OPTION in
         u)hadoop_user=$OPTARG
           ;;
         d)log_date=$OPTARG
           OPT=yes
           ;;
         t)table=$OPTARG
           OPT=yes
           ;;
         \?) usage
           ;;
      esac
   done

   if [ "$OPT" = "no" ]
   then
      usage
   fi

   valid

   #log "============================================================================"
   #log "运行脚本：$0 业务日期：$log_date  Hadoop用户：$hadoop_user"
   
   log_info "============================================================================"
   log_info "运行脚本：$0 业务日期：$log_date  Hadoop用户：$hadoop_user"
}


usage_gp()
{
   echo "命令行参数有误"
   echo "`basename $0` -d yyyymmdd [-l gp accessid]"
   exit 1
}

#GP脚本命令行参数解释，使用该函数时传入参数$*
gp_arg()
{
   unset OPTIND
  
   #gp_link=GP_LDSDB_APP_SIAM
   gp_link=GP_SORDB_BASE
   OPT=no
   while getopts :l:d:t: OPTION
   do
      case $OPTION in
         l)gp_link=$OPTARG
           ;;
         d)log_date=$OPTARG
           OPT=yes
           ;;
         t)table_name=$OPTARG
           ;;
         \?) usage_gp
           ;;
      esac
   done

   if [ "$OPT" = "no" ]
   then
      usage_gp
   fi

   valid_gp
   	
   log "============================================================================"
   log "运行脚本：$0 业务日期：$log_date  GP AccessId连接：$gp_link"
}


#登录日志记录
log_info(){
   logTime=`date "+%Y-%m-%d %H:%M:%S"`
   return_msg=$*
   echo "$logTime $execute PID:$$ $return_msg" >> $logpath/case_trace.$curDate
}

#Hadoop用户登录
hadoop_login()
{
   if [ "$HADOOP_ENV" = "ts150" ]; then
      source /home/ap/dip/hadoop/hadoop_client_aq/bigdata_env

      uid=`id -u`
      export KRB5CCNAME=/tmp/TS150_aq_keytab_$uid

      kinit -k ts150 -t /home/ap/dip_ts150/hadoop_aq_keytab/ts150/user.keytab
   else
      hadoop_login_old
   fi
}
#旧集群用户登录
hadoop_login_old()
{
   source /home/ap/dip/hadoop/hadoop_client/bigdata_env
   uid=`id -u`
   export KRB5CCNAME=/tmp/TS150_hds_keytab_$uid
   kinit -k TS150 -t /home/ap/dip/hadoop/keytab/TS150.keytab

}

p2log_usage()
{
   echo "命令行参数有误"
   echo "`basename $0` -d yyyymmdd [-h host_name]"
   exit 1
}

#脚本命令行参数解释，使用该函数时传入参数$*
p2log_arg()
{
   unset OPTIND
   OPT=no
   while getopts :d:h: OPTION
   do
      case $OPTION in
         d)log_date=$OPTARG
           OPT=yes
           ;;
         h)host_name=$OPTARG
           ;;
         \?) usage
           ;;
      esac
   done

   if [ "$OPT" = "no" ]; then
      usage
   fi

   if [ ! `echo $log_date | egrep "^20[0-9]{2}[0-1][0-9][0-3][0-9]$"` ]; then
      echo "输入日期格式错:$log_date"
      usage
   fi

   statuspath=script_path/status/$log_date
   if [ ! -d $statuspath ]; then
      mkdir -p $statuspath
   fi
   status_1_uncompress_over=$statuspath/1_uncompress_over.txt
   status_2_input=$statuspath/2_put_hdfs_in.txt
   status_2_over=$statuspath/2_put_hdfs_over.txt
   status_2_old_over=$statuspath/2_put_old_hdfs_over.txt
   # 从结构化数据后，需顺序处理
   # hive_insert_over hive_struct_over hive_export_over gp_partition_over gp_load_data_over gp_index_over
   status_3_over=$statuspath/3_progress_complete.txt
   status_4_over=$statuspath/4_export_over.txt
   status_5_over=$statuspath/5_load_to_gp.txt
   status_6_over=$statuspath/6_insert_gp_over.txt
   status_7_over=$statuspath/7_clean_data_over.txt
   # 状态文件不存在，则新建
   if [ ! -f $status_1_uncompress_over ]; then
      touch $status_1_uncompress_over
   fi
   if [ ! -f $status_2_input ]; then
      touch $status_2_input
   fi
   if [ ! -f $status_2_over ]; then
      touch $status_2_over
   fi
   if [ ! -f $status_2_old_over ]; then
      touch $status_2_old_over
   fi
   if [ ! -f $status_3_over ]; then
      touch $status_3_over
   fi
   if [ ! -f $status_4_over ]; then
      touch $status_4_over
   fi
   if [ ! -f $status_5_over ]; then
      touch $status_5_over
   fi
   if [ ! -f $status_6_over ]; then
      touch $status_6_over
   fi
   chmod 777 $status_5_over
   chmod 777 $status_4_over
   chmod 777 $status_3_over
   chmod 777 $status_6_over
   chmod 777 $status_2_old_over
}   

#导出GP表
export_gp_table()
{
   tablename=$1
   condition=${2:-"1=1"}
   local data_date=${3:-$log_date}
   gp_link=${4:-GP_SORDB_BASE}

   filename=TS150_SAFEID_000000000_${tablename}_${data_date}_0001.dat

   perl /home/ap/dip/bin/export_data.pl $gp_link $hdsinput/$data_date/$filename $tablename "$condition" N "\001" Y utf8 1 N "" N ./ Y

   return $?
}

log()
{
   local msg=$1
   local ret=${2:-$?}
   local level=${3:-error}
   local pid=$$
   local ppid=`ps -p $pid -o ppid=`
   local ppname=`ps -p $ppid -o comm=`
   local user=`ps -p $pid -o user=`
   
   if [ "$0" = "-bash" ]; then
      running_path=$(cd `dirname $BASH_SOURCE`; pwd)
      local name=`basename $BASH_SOURCE`
   else
      running_path=$(cd `dirname $0`; pwd)
      local name=`basename $0`
   fi

   out=`sqlite3 $script_path/violate.db "select ts, id from log where id = (select max(id) from log where pid=$pid and name='$name');"`
   if [ "$out" != "" ]; then
      prev_id=`echo $out | awk -F "|" '{print $2}'`
      prev_ts=`echo $out | awk -F "|" '{print $1}'`
      echo "has max id: $prev_id, $prev_ts"
   fi

   sqlite3 $script_path/violate.db "insert into log(msg, ret, pid, path, name, args, ppid, ppname, user, level) values('$msg', $ret, $pid, '$running_path', '$name', '$args', $ppid, '$ppname', '$user', '$levle')"
   return $?
}

log_error()
{
   log $*
   return $?
}