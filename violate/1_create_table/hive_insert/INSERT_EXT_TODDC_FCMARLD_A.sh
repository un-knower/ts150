#!/bin/sh
######################################################
#   重要非帐务性流水: EXT_TODDC_FCMARLD_A临时表导入到贴源表中
#                   wuzhaohui@tienon.com
######################################################

#引用基础Shell函数库
source /home/ap/dip/appjob/shelljob/TS150/violate/base.sh

#登录Hadoop
hadoop_login

#解释命令行参数
logdate_arg $*

# 依赖数据源--当天数据
IN_CUR_HIVE=
IN_CUR_HDFS=/bigdata/input/TS150/case_trace/TODDC_FCMARLD_A/

# Hive输出表，判断脚本是否已成功运行完成
OUT_CUR_HIVE=INN_TODDC_FCMARLD_A

run()
{
   beeline -f $script_path/1_create_table/hive_insert/INSERT_EXT_TODDC_FCMARLD_A.sql --hivevar log_date=${log_date}

   return $?
}
