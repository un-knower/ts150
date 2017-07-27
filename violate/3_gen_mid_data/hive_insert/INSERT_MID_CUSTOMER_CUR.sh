#!/bin/sh
######################################################
#   对私客户信息整合表，带原系统客户号: MID_CUSTOMER_CUR中间表整合处理
#                   wuzhaohui@tienon.com
######################################################

#引用基础Shell函数库
source /home/ap/dip/appjob/shelljob/TS150/violate/base.sh

#登录Hadoop
hadoop_login

#解释命令行参数
logdate_arg $*

# 依赖数据源--当天数据
IN_CUR_HIVE="T0042_TBPC9030_H T0042_TBPC1010_H"

# Hive输出表，判断脚本是否已成功运行完成
OUT_CUR_HIVE="MID_CUSTOMER_CUR"

run()
{
   beeline -f $script_path/3_gen_mid_data/hive_insert/INSERT_MID_CUSTOMER_CUR.sql --hivevar log_date=${log_date}
   return $?
}
