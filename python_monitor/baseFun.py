#!/usr/bin/env python
#coding:utf8

import os, sys, re
sys.path.append("../python_common/")
from common_fun import *


# 获取SQL脚本文件中的SQL语句
def getSql(fileName):
    all_lines = ""
    if os.path.exists(fileName):
        f = open(fileName, "r")

        for line in f.readlines():
            stmt = line.split('--')[0]
            all_lines += stmt

        # for sql in all_lines.split(';'):
        #     self.conn.execute(sql)

        f.close()
    sql_array = []
    for sql in all_lines.split(';'):
        if sql.strip() != '':
            sql_array.append(sql.strip())

    return sql_array


# 获取SQL脚本文件中的注释语句中包含k-v
def getRemarkKV(fileName, remark_flag=None):
    ret_kv_array = []
    if os.path.exists(fileName):
        f = open(fileName, "r")

        for line in f.readlines():
            if remark_flag:
                line_array = line.split(remark_flag)
                if len(line_array) <= 1:
                    continue
                stmt = line_array[1]
            else:
                stmt = line
            kv_array = stmt.split('=')
            if len(kv_array) <= 1 or kv_array[1].strip() == '':
                continue

            # 需要处理表达式中的等号
            express = '='.join(kv_array[1:]).strip().replace('"', '').replace("'", "")

            ret_kv_array.append((kv_array[0], express))

        f.close()

    return ret_kv_array


# 获取SQL脚本文件中变量
def getVar(fileName):
    var_list = []
    patt = re.compile(r'\$\{(\w+)\}', re.M)

    for line in getSql(fileName):
        # print '[%s]' % line

        var_list.extend(patt.findall(line))

    return set(var_list)


# 通知信息发送
def notice(row, over_status, succ_msg=None, fail_msg=None, logFile=None):
    if not succ_msg and not fail_msg:
        return

    msg = None
    if row.get('error_notice', False) and over_status != 'over':
        msg = fail_msg

    if row.get('over_notice', False) and over_status == 'over':
        msg = succ_msg

    if not msg:
        log.debug('不需要发送信息:error_notice[%s] over_notice[%s] [%s]' % (
            row.get('error_notice', False), row.get('over_notice', False),
            over_status), logFile)
        return

    # 手机号
    patt_mobile = re.compile(r'^1[3578]\d{9}$')
    # 邮箱
    patt_email = re.compile(r'[0-9a-zA-Z.]+@[0-9a-zA-Z.]+')

    # 对所有接收人发信息
    for receiver in notice_receiver:
        cmd = None
        send_cmd = 'curl http://11.168.35.34:8101/uassService/uassserviceaction.action -d uaap_request_result=json -d userName=TS150 -d opName=SMS -d operator=TS150'
        m = patt_mobile.match(receiver)
        if m:
            log.info('发送短信:[%s][%s]' % (receiver, msg), logFile)
            cmd = send_cmd + ' -d _fw_service_id=sendSms -d handSet=%s -d smsContent="%s"' % (receiver, msg)

        m = patt_email.match(receiver)
        if m:
            log.info('发送邮件:[%s][%s]' % (receiver, msg), logFile)
            cmd = send_cmd + ' -d _fw_service_id=sendMail -d mailbox=%s -d mailContent="%s" -d mailTitle="%s"' % (receiver, msg, '安全监控组件告警')

        if cmd:
            # 运行脚本
            (returncode, out_lines) = executeShell(cmd)
            log.info('通知信息发送完成:[%s][%d]' % (cmd, returncode), logFile)


# 按表格样式显示
def display_table(row_array, field_list=None):
    if not field_list:
        field_list = row_array[0].keys()

    # 插入标题行
    change_map = {'ts':'时间戳', 'ts_short':'时间戳', 'base_script':'脚本',
                  'start_date':'起始日期', 'end_date':'终止日期',
                  'hostname':'主机', 'username':'用户',
                  'step':'步长', 'force':'强制',
                  'over_notice':'完成通知', 'error_notice':'出错通知',
                  'process_date':'处理日期', 'over_date':'完成日期',
                  'status':'状态', 'pid_exist':'进程存在',
                  'next_action':'等待动作', 'pre_work_id':'跟随作业',
                  'crontab':'定时'}

    # 标题行转换
    title_map = {}
    for field in field_list:
        title_map[field] = change_map.get(field, field)
    row_array.insert(0, title_map)

    # 计算最大宽度
    field_max_len = {}
    for field in field_list:
        for row in row_array:
            row_str = str(row[field])
            if char_length(row_str) >= field_max_len.get(field, 0):
                field_max_len[field] = char_length(row_str)
    # print field_max_len

    for row in row_array:
        line = ''
        for field in field_list:
            row_str = str(row[field])
            # print '[%s][%d][%d]' % (row_str.ljust(field_max_len[field] + 1), field_max_len[field], len(row_str))
            line += char_ljust(row_str, field_max_len[field] + 1)
        if isLinux():
            print line
        else:
            print line.decode('utf-8').encode('gbk')
