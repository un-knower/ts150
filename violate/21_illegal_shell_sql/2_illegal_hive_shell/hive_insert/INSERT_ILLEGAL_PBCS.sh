#!/bin/sh
######################################################
# PBCS系统 无卡异地查询分析
######################################################

#引用基础Shell函数库
source /home/ap/dip_ts150/ts150_script/base.sh

#登录Hadoop
hadoop_login

#解释命令行参数
logdate_arg $*


# 依赖数据源--昨天数据
IN_PRE_HIVE=ILLEGAL_PBCS_A
IN_CUR_HDFS=

# Hive输出表，判断脚本是否已成功运行完成
OUT_CUR_HIVE=ILLEGAL_PBCS_A

run()
{
   beeline -f ./hive_insert/INSERT_ILLEGAL_PBCS.sql --hivevar p9_data_date=${log_date}
}


