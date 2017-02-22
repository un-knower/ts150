#!/bin/sh

######################################################
# ccbs 综合前置 无卡异地查询分析
######################################################

#新规则描述：
# 1.查询交易限制：tx_code=A00421502|A00421517|A00421538|A00421547
# 2.无卡查询规则：
# 1）customer_identity_no为空 且 customer_acctount_no为空 则为无卡查询 
# 3) 填充客户身份证id_crdt_no字段、客户号CST_ID、卡号fs_acct_no字段，完善客户信息
# 2）根据卡号添加开户行，筛选无卡异地查询
# 4）添加其他信息并统计



#引用基础Shell函数库
source /home/ap/dip_ts150/ts150_script/base.sh

#登录Hadoop
hadoop_login

#解释命令行参数
logdate_arg $*


# 依赖数据源--昨天数据
IN_PRE_HIVE=ILLEGAL_P2NOCARD_A
IN_CUR_HDFS=

# Hive输出表，判断脚本是否已成功运行完成
OUT_CUR_HIVE=ILLEGAL_P2NOCARD_A

run()
{
   beeline -f ./hive_insert/INSERT_ILLEGAL_P2NOCARD_A.sql --hivevar p9_data_date=${log_date}
}


