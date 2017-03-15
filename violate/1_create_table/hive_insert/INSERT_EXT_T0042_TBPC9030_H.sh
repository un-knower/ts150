#!/bin/sh
######################################################
#   内外客户号对照信息: EXT_T0042_TBPC9030_H临时表导入到贴源表中
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
IN_CUR_HDFS=/bigdata/input/TS150/case_trace/T0042_TBPC9030_H/

# Hive输出表，判断脚本是否已成功运行完成
OUT_CUR_HIVE=INN_T0042_TBPC9030_H

run()
{
   beeline -f $script_path/1_create_table/hive_insert/INSERT_EXT_T0042_TBPC9030_H.sql --hivevar log_date=${log_date}
   # ret=$?

   # 插入成功则删除HDFS源文件
   # if [ $ret -eq 0 ]; then
   #    hadoop fs -rm -r -f -skipTrash /bigdata/input/TS150/case_trace/T0042_TBPC9030_H
   # fi
   return $?
}
