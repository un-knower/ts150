#!/bin/sh
######################################################
#   取得客户的排队信息
######################################################

#引用基础Shell函数库
source /home/ap/dip_ts150/ts150_script/base.sh

#登录Hadoop
hadoop_login

#解释命令行参数
logdate_arg $*


# 依赖数据源--当天数据
IN_PRE_HIVE=ILLEGAL_CUST_QUEUE_INFO
IN_CUR_HDFS=

# Hive输出表，判断脚本是否已成功运行完成
OUT_CUR_HIVE=ILLEGAL_CUST_QUEUE_INFO

run()
{
   beeline -f ./hive_insert/INSERT_ILLEGAL_CUST_QUEUE_INFO.sql --hivevar p9_data_date=${log_date}
}
