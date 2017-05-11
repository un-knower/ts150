#!/usr/bin/python
#coding:utf8

# 自定义公用变量

# 应用名称
app = "train"

# 默认运行脚本目录
#base_path=/home/ap/dip/appjob/shelljob/TS150/violate
base_path = "/home/ap/dip_ts150/ts150_script/ccb_risk_scoring"
# base_path = r"G:/1_Tienon/3_安全监控/模型分析/git/non_self".decode('utf8').encode('gbk')
base_path = ".."

# 运行目录
run_path = "%s/train" % base_path

# 默认Hive数据库名
default_hive_db = "sor"

# 默认GP数据库名
default_gp_schema = "train"

# 日志级别
log_level = 'debug'
