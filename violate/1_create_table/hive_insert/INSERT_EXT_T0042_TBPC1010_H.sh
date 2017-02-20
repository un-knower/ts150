#!/bin/sh
######################################################
#   客户基本信息: EXT_T0042_TBPC1010_H临时表导入到贴源表中
#                   wuzhaohui@tienon.com
######################################################

#引用基础Shell函数库
source /home/ap/dip_ts150/ts150_script/base.sh

#登录Hadoop
hadoop_login

#解释命令行参数
logdate_arg $*

# 依赖数据源--当天数据
IN_CUR_HIVE=
IN_CUR_HDFS=/bigdata/input/TS150/case_trace/T0042_TBPC1010_H/

# Hive输出表，判断脚本是否已成功运行完成
OUT_CUR_HIVE=INN_T0042_TBPC1010_H

run()
{
   beeline -f ./hive_insert/INSERT_EXT_T0042_TBPC1010_H.sql --hivevar log_date=${log_date}
}
