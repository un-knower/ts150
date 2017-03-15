#!/bin/sh
######################################################
#   CCBS帐户与卡整合表: MID_CARD_CUR中间表整合处理
#                   wuzhaohui@tienon.com
######################################################

#引用基础Shell函数库
source /home/ap/dip_ts150/ts150_script/base.sh

#登录Hadoop
hadoop_login

#解释命令行参数
logdate_arg $*

# 依赖数据源--当天数据
IN_CUR_HIVE="TODDC_SAACNACN_H TODDC_CRCRDCRD_H"

# Hive输出表，判断脚本是否已成功运行完成
OUT_CUR_HIVE=MID_CARD_CUR

run()
{
   beeline -f ./hive_insert/INSERT_MID_CARD_CUR.sql --hivevar log_date=${log_date}
}
