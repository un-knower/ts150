#!/usr/bin/python
#coding:utf8

# 自定义公用变量

# 运行模式, sequential 连续, timer 定时
run_mode = 'sequential'

# 是否测试环境
is_test = True

# 应用名称
app = "train"

# 默认运行脚本目录
#base_path=/home/ap/dip/appjob/shelljob/TS150/violate
base_path = "/home/ap/dip_ts150/ts150_script/ccb_risk_scoring"
base_path = "../test"

# 运行目录
run_path = "%s" % base_path

# 默认Hive数据库名
default_hive_db = "test"

# 默认GP数据库名
default_gp_schema = "app_siam"

# 日志级别
log_level = 'debug'
# log_level = 'info'
# log_level = 'error'

# 作业起始日期，该日期前一天的数据不检查是否存在
app_start_date = '20151106'

# 通知消息接收人，可以是手机号或邮箱
notice_receiver = ['18159283921', 'wuzhaohui@tienon.com']

# 开启HTTP服务器地址端口
http_ip = '127.0.0.1'
http_port = 29150
