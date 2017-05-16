#!/usr/bin/python
#coding:utf8

# 自定义公用变量

# 应用名称
app = "train"

# 默认运行脚本目录
#base_path=/home/ap/dip/appjob/shelljob/TS150/violate
base_path = "/home/ap/dip_ts150/ts150_script/ccb_risk_scoring"
#base_path = ".."

# 运行目录
run_path = "%s/train" % base_path

# 默认Hive数据库名
default_hive_db = "train"

# 默认GP数据库名
default_gp_schema = "train"

# 日志级别
#log_level = 'debug'
log_level = 'info'

# 作业起始日期，该日期前一天的数据不检查是否存在
app_start_date = '20151106'
