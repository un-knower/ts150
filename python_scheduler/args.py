#!/usr/bin/python
#coding:utf8

import os
import time
from optparse import OptionParser
from multiprocessing import Process
import dbScheduler
from var import *
from scheduler import *
import crontab
sys.path.append("../python_common/")
import log
from common_fun import *


# 实体检查子进程处理
def check_depend_entity_process(db, row):
    sched = Scheduler(db=db)
    valid_success = sched.check_entity(row['entity'], row['data_date'], flag=row['flag'])

    return valid_success


# 依赖项实体表--数据检查
def check_depend_entity(db, id=0):
    job_list = []
    row_map = {}
    if id > 0:
        row_map = db.query_entity(id)
    else:
        # 时间差大于15分钟
        # minute_interval = "julianday('now', 'localtime')*1440 - julianday(ts)*1440 > 15"
        # where = "status<>'exist' and (status='processing' and %s) " % minute_interval

        # 取每个实体不存在记录中，最早的一天
        where = "status<>'exist'"
        sql = "select flag, entity, min(data_date) as min_data_date from entity where %s group by flag, entity" % where
        row_array = db.query_sql(sql)

        for row in row_array:
            again_row_map = db.query_entity(where="where flag='%s' and entity='%s' and data_date='%s'" % \
                                            (row['flag'], row['entity'], row['min_data_date']))

            row_map.update(again_row_map)

    for row in row_map.values():
        # 正在处理中，且进程存在，则排除
        if row['status'] == 'processing' and exist_pid(row['pid']):
            continue

        job = Process(target=check_depend_entity_process, args=(db, row), name="%s_%s" % (row['entity'], row['data_date']))
        job.start()
        job_list.append(job)

    return job_list


# 运行作业
def run_work(db, id=None):
    log.debug('运行跑批作业调度')
    job_list = []
    if id:
        row_map = db.query_work_config(id=id)
    else:
        where="where (over_date<end_date or (over_date=end_date and status<>'over')) " + \
              "  and hostname='%s' and username='%s'" % (hostname, username)
        row_map = db.query_work_config(where=where)
    rn_list = row_map.keys()
    rn_list.sort()

    for rn in rn_list:
        row = row_map[rn]
        options = row['options']
        script = options['script']

        # log.debug('找到跑批作业：%s,%s,%s' % (script, row['status'], row['pid']))
        log.info('找到跑批作业：%s' % (script), script)

        # 正在处理中，且进程存在，则排除
        if row['status'] in ('processing', 'checking') and exist_pid(row['pid']):
            log.info('作业：%s正在处理中，跳过' % (script), script)
            continue

        # 作业暂停，则排除
        if row['status'] in ('pause', ):
            log.info('作业：%s暂停，跳过' % (script), script)
            continue

        # 作业日期到今天或昨天，检查时间延长到1小时1次
        yesterday = dateCalc(today(), -1)
        if row['over_date'] in (today(), yesterday):
            minuteDelta = timeCalc(row['ts'], 'minute')
            if minuteDelta < 60:
                log.info('作业：%s检查时间延长到1小时，跳过' % (script), script)
                continue

        log.info('开始运行子进程:id[%d] script[%s] start[%s] end[%s] over[%s] status[%s]' % (
            row['id'], script, row['start_date'], row['end_date'], row['over_date'], row['status']), script)

        job = Process(target=run_work_process, args=(db, row), name="%s_%s" % (script, row['start_date']))
        job.start()
        job_list.append(job)

    return job_list


# 运行从起始日期到终止日期作业的子进程
def run_work_process(db, row):
    log.debug("子进程开始执行作业:%s" % row)
    options = row['options']
    scriptName = options['script']

    # 断点重跑
    if row['over_date'] == '':
        start_date = row['start_date']
    else:
        if row['status'] == 'over':
            start_date = dateCalc(row['over_date'], 1)
        else:
            start_date = row['over_date']

    if start_date > row['end_date']:
        log.info('%s[%s][%s]已跑批结束，退出' % (scriptName, start_date, row['end_date']), scriptName)
        return

    log.info('%s[%s][%s]开始跑批' % (scriptName, start_date, row['end_date']), scriptName)
    # 按日期一天天往下跑
    for data_date in getDateList(start_date, row['end_date']):
        over_status = run_work_one_date(db, row, data_date)
        if 'over' != over_status:
            break

    notice(options, over_status, '连续跑批脚本执行完成[%s][%s][%s]' % (scriptName, start_date, row['end_date']),
                                 '连续跑批脚本执行失败[%s][%s][%s]' % (scriptName, start_date, row['end_date']),
                                 logFile=scriptName)


# 跑一天的脚本
def run_work_one_date(db, row, data_date):
    sched = Scheduler(db=db)
    options = row['options']
    scriptName = options['script']

    log.info('准备开始跑脚本:[%s]处理[%s]的数据' % (scriptName, data_date), scriptName)

    over_status = 'checking'
    db.update_work_config(row['id'], over_date=data_date, status=over_status, pid=os.getpid())

    # 输入依赖检查
    if sched.check_script_depend(row['script'], data_date, io='in', viaTable=not options['force']):
        db.update_work_config(row['id'], over_date=data_date, status='processing', pid=os.getpid())

        log.info('输入检查通过，开始跑脚本:[%s][%s]' % (scriptName, data_date), scriptName)
        # 脚本类型
        script_type = get_script_type(scriptName)
        if script_type == '.sh':
            over_status = sched.run_shell(row, data_date)
        elif script_type == '.py':
            over_status = sched.run_python(row, data_date)
        elif script_type == '.sql':
            over_status = sched.run_hive_sql(row, data_date)
        elif script_type == '.gpsql':
            over_status = sched.run_gp_sql(row, data_date)

        log.info('跑脚本:[%s][%s]完成:[%s]' % (scriptName, data_date, over_status), scriptName)

        # 输出结果检查
        if over_status == 'over' and not sched.check_script_depend(row['script'], data_date, io='out'):
            over_status = 'non-result'

    else:
        over_status = 'non-depend'

    db.update_work_config(row['id'], over_date=data_date, status=over_status, pid=os.getpid())

    return over_status


# 跑定时脚本
def run_crontab_process(db, row, data_date):
    sched = Scheduler(db=db)
    options = row['options']
    scriptName = options['script']

    log.info('准备开始跑脚本:[%s]处理[%s]的数据' % (scriptName, data_date), scriptName)

    over_status = 'checking'
    db.save_crontab_config(id=row['id'], status=over_status, pid=os.getpid())

    # 输入依赖检查
    if sched.check_script_depend(row['script'], data_date, io='in', viaTable=not options['force']):
        db.save_crontab_config(id=row['id'], status='processing', pid=os.getpid())

        log.info('输入检查通过，开始跑脚本:[%s][%s]' % (scriptName, data_date), scriptName)
        # 脚本类型
        script_type = get_script_type(scriptName)
        if script_type == '.sh':
            over_status = sched.run_shell(row, data_date)
        elif script_type == '.py':
            over_status = sched.run_python()
        elif script_type == '.sql':
            over_status = sched.run_hive_sql(row, data_date)
        elif script_type == '.gpsql':
            over_status = sched.run_gp_sql(row, data_date)

        log.info('跑脚本:[%s][%s]完成:[%s]' % (scriptName, data_date, over_status), scriptName)

        # 输出结果检查
        if over_status == 'over' and not sched.check_script_depend(row['script'], data_date, io='out'):
            over_status = 'non-result'

    else:
        over_status = 'non-depend'

    db.save_crontab_config(id=row['id'], status=over_status, pid=os.getpid())

    notice(options, over_status, '定时脚本检查通过[%s][%s]' % (scriptName, data_date),
                                 '定时脚本检查不通过[%s][%s]' % (scriptName, data_date),
                                 logFile=scriptName)

    return over_status


# 通知信息发送
def notice(options, over_status, succ_msg=None, fail_msg=None, logFile=None):
    if not succ_msg and not fail_msg:
        return

    msg = None
    if options['error_notice'] and over_status != 'over':
        msg = fail_msg

    if options['over_notice'] and over_status == 'over':
        msg = succ_msg

    if not msg:
        log.info('不需要发送信息:error_notice[%s] over_notice[%s] [%s]' % (
            options['error_notice'], options['over_notice'], over_status), logFile)
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


# 连续作业守护进程
def daemon():
    db = DbHelper()

    job_list = []
    while True:
        # 依赖实体检查
        check_job_list = []
        # check_job_list = check_depend_entity(db)

        # 运行作业
        run_job_list = run_work(db)

        job_list.extend(check_job_list)
        job_list.extend(run_job_list)

        # 检查进程完成情况
        over_job_list = []
        for job in job_list:
            job.join(1)
            if job.is_alive():
                # print "pid:%d, name:%s is_alive" % (job.pid, job.name)
                pass
            else:
                log.info("进程号:%s, pid:%d, exit code:%d" % (job.name, job.pid, job.exitcode))
                over_job_list.append(job)

        # 删除已完成的作业
        for over_job in over_job_list:
            job_list.remove(over_job)

        # break
        sys.stdout.flush()
        sys.stderr.flush()
        time.sleep(60)


# 定时任务守护进程
def timer():
    db = DbHelper()

    job_list = []
    while True:
        # 查找所有不是正在跑的脚本
        where = "where hostname='%s' and username='%s'" % (hostname, username)
        row_map = db.query_crontab_config(where=where)

        for row in row_map.values():
            options = row['options']
            script = options['script']

            # 正在处理中，且进程存在，则排除
            if row['status'] in ('processing', 'checking') and exist_pid(row['pid']):
                log.info('作业：%s正在处理中，跳过' % (script), script)
                continue

            # 运行作业
            if crontab.nowisdo(row['crontab']):
                log.info('定时[%s]到了，开始执行脚本[%s]' % (row['crontab'], script))
                data_date = today()

                job = Process(target=run_crontab_process, args=(db, row, data_date), name="%s_%s" % (script, data_date))
                job.start()
                job_list.append(job)

        # 检查进程完成情况
        over_job_list = []
        for job in job_list:
            if job in over_job_list:
                continue

            job.join(0.1)
            if not job.is_alive():
                log.info("进程号:%s, pid:%d, exit code:%d" % (job.name, job.pid, job.exitcode))
                over_job_list.append(job)

        # 删除已完成的作业
        for over_job in over_job_list:
            job_list.remove(over_job)

        # break
        sys.stdout.flush()
        sys.stderr.flush()
        time.sleep(60)


def list_work_all(*args, **kwargs):
    list_work(detail=True)


def list_work_processing(*args, **kwargs):
    list_work(where="where status<>'over'")


def list_work(where='', detail=False):
    db = dbScheduler.DbScheduler(remote_mode=False)
    row_array = db.query_work_config(where=where)
    for row in row_array:
        # 非本节点运行的进程，不检查进程是否存在
        if row['hostname'] == hostname:
            pid_str = 'Has' if row['pid'] and exist_pid(row['pid']) else 'No'
        else:
            pid_str = ''
        row['pid_exist'] = pid_str
        row['ts'] = row['ts'][11:]
        row['force'] = 'Force' if row['force'] else ''
        row['over_notice'] = 'over_notice' if row['over_notice'] else ''
        row['error_notice'] = 'error_notice' if row['error_notice'] else ''


    display_table(row_array,
                  ['id', 'ts', 'base_script',
                   'start_date', 'end_date', 'hostname', 'username',
                   'step', 'force', 'over_notice', 'error_notice',
                   'process_date', 'over_date', 'status', 'pid', 'pid_exist',
                   'next_action', 'pre_work_id'])
    return

    # 计算最大脚本名称宽度
    max_script_len = 0
    for row in row_array:
        if len(row['base_script']) > max_script_len:
            max_script_len = len(row['base_script'])

    if detail:
        print('ID  %-23s%s%s %s %s %s    %s   %s          %s  %s' % (
            '时间戳', '脚本名'.ljust(max_script_len + 4), '起始日期', '终止日期',
            '完成日期', '处理状态', '进程号', '主机', '用户名', '进程存在'))
    else:
        print('ID  %s   %s%s %s %s %s %s   %s          %s  %s' % (
            '时间戳', '脚本名'.ljust(max_script_len + 4), '起始日期', '终止日期',
            '完成日期', '处理状态', '主机', '用户名', '进程号', '存在'))

    for row in row_array:
        script = row['base_script'].ljust(max_script_len + 1)

        # 非本节点运行的进程，不检查进程是否存在
        if row['hostname'] == hostname:
            pid_str = '存在' if row['pid'] and exist_pid(row['pid']) else '不存在'
        else:
            pid_str = ''

        if detail:
            pass
        else:
            ts = row['ts'][11:]
            print('%-4d%-9s%s%-9s%-9s%-9s%-12s%-7s%-16s%-6s' % (row['id'], ts, script,
                  row['start_date'], row['end_date'], row['over_date'], row['status'],
                  row['hostname'], row['username'], row['pid'], pid_str))
        # print row
    exit(0)


def display_table(row_array, field_list=None):
    if not field_list:
        field_list = row_array[0].keys()

    # 插入标题行
    change_map = {'base_script':'脚本',
                  'start_date':'起始日期',
                  'end_date':'终止日期',
                  'hostname':'主机',
                  'username':'用户',
                  'step':'步长',
                  'force':'强制',
                  'over_notice':'完成通知',
                  'error_notice':'出错通知',
                  'process_date':'处理日期',
                  'over_date':'完成日期', 'status':'状态', 'pid_exist':'进程存在',
                  'next_action':'等待动作', 'pre_work_id':'跟随作业'}

    title_map = dict(zip(field_list, field_list))
    row_array.insert(0, title_map)

    # 计算最大宽度
    field_max_len = {}
    for field in field_list:
        for row in row_array:
            row_str = str(row[field])
            if len(row_str) >= field_max_len.get(field, 0):
                field_max_len[field] = len(row_str)
    print field_max_len

    for row in row_array:
        line = ''
        for field in field_list:
            row_str = str(row[field])
            line += row_str.ljust(field_max_len[field] + 1)
        print line


def debug_work(*args, **kwargs):
    log.debug('debug_work')
    timer()
    exit(0)


# 校验并调整输入格式
def valid(options, db):
    if options.start_date and not dateValid(options.start_date):
        print '日期格式有误:[%s]' % (options.start_date)
        return False

    if options.end_date and not dateValid(options.end_date):
        print '日期格式有误:[%s]' % (options.end_date)
        return False

    if options.start_date and not options.end_date:
        options.end_date = options.start_date

    if options.crontab:
        #解析crontab时间配置参数 分 时 日 月 周 各个取值范围
        res, cron_time = crontab.parse_crontab_time(options.crontab)
        if res != 0:
            print 'crontab时间配置有误:[%s]' % (options.crontab)
            return False

    # 指定运行主机与用户
    if options.username:
        tmp_array = options.username.split('.')
        if len(tmp_array) == 2:
            assign_hostname = tmp_array[0]
            assign_username = tmp_array[1]
            options.username = '@'.join([assign_username, assign_hostname])

        tmp_array = options.username.split('@')
        if len(tmp_array) != 2:
            print '指定运行主机格式不符:[%s]' % options.username
            return False
    else:
        options.username = '@'.join([username, hostname])

    # script 新增作业
    if options.script:
        # 找到脚本文件
        script = findFile(run_path, options.script)
        if not script:
            print '--script 脚本:[%s]在目录:[%s]未找到' % (run_path, options.script)
            return False

        options.script = script

    for task_id in (options.task_id, options.pre_work_id):
        if task_id:
            row_array = db.query_work_config(id=task_id)

            if len(row_array) == 0:
                print '指定任务ID:%d不存在' % task_id
                return False

    return True


def main():
    parser = OptionParser(usage='usage: %prog [options]')
    parser.add_option("-c", "--script", dest="script", help=u"脚本名称")
    parser.add_option("-r", "--crontab", dest="crontab", help=u"crontab定时配置字符串")
    parser.add_option("-s", "--start_date", dest="start_date", help=u"起始日期")
    parser.add_option("-e", "--end_date", dest="end_date", help=u"终止日期，默认与起始日期一致")
    parser.add_option("-f", "--force", action="store_true", default=False, dest="force", help=u"强制重跑")
    parser.add_option("-t", "--task", type="int", default=0, dest="task_id", help=u"指定任务ID，断点重跑")
    parser.add_option("-u", "--username", dest="username", help=u"指定运行任务的主机与用户 hostname.username")
    parser.add_option("--flow_work", type="int", default=0, dest="pre_work_id", help=u"指定跟随任务ID")
    parser.add_option("--step", type="int", default=1, dest="step", help=u"指定下一日期的步长")
    parser.add_option("--error_notice", action="store_true", default=False, dest="error_notice", help=u"出错时短信邮件通知")
    parser.add_option("--over_notice", action="store_true", default=False, dest="over_notice", help=u"完成时短信邮件通知")
    # parser.add_option("--kill", action="callback", callback=stop, help=u"停止调试进程")
    parser.add_option("--deltask", action="store_true", default=False, dest="delete_task", help=u"删除作业配置")
    parser.add_option("--pause", action="store_true", default=False, dest="pause_task", help=u"暂停作业执行")
    parser.add_option("--resume", action="store_true", default=False, dest="resume_task", help=u"恢复作业执行")
    parser.add_option("--run", action="store_true", default=False, dest="run_task", help=u"本地调试执行作业")

    parser.add_option("-l", "--list", action="callback", callback=list_work_processing, help=u"列出调度任务运行状态")
    parser.add_option("--la", action="callback", callback=list_work_all, help=u"列出调度任务运行状态")
    parser.add_option("--debug", action="callback", callback=debug_work, help=u"调试模式")

    (options, args) = parser.parse_args()

    db = dbScheduler.DbScheduler(remote_mode=False)
    # 校验输入项
    if not valid(options, db):
        return


    # 指定运行主机与用户
    if options.username:
        tmp_array = options.username.split('@')
        assign_hostname = tmp_array[1]
        assign_username = tmp_array[0]

    # 指定任务ID跑批
    if options.task_id:
        print '指定任务ID:%d' % options.task_id

        # 删除作业
        if options.delete_task:
            db.delete_work_config(id=options.task_id)
            return
        # 暂停作业
        if options.pause_task:
            db.update_work_config(id=options.task_id, next_action='pause')
            return
        # 恢复作业
        if options.resume_task:
            db.update_work_config(id=options.task_id, next_action='resume')
            return

        row_array = db.query_work_config(id=options.task_id)
        row = row_array[0]
        next_action = None
        # 重新指定起始日期
        if options.start_date:
            start_date = options.start_date
        else:
            # 强制从第一天开始跑
            if options.force:
                start_date = row['start_date']
                next_action = 'init'
                row['force'] = True
            else:
                start_date = row['over_date']

        # 重新指定终止日期
        if options.end_date:
            end_date = options.end_date
        else:
            end_date = row['end_date']

        # 更新作业配置表
        db.update_work_config(options.task_id, over_date=start_date, next_action=next_action, end_date=end_date)

        # 调试作业
        if options.run_task:
            # 开始子进程跑批
            job_list = run_work(db, options.task_id)
            for job in job_list:
                while True:
                    job.join(1)
                    if not job.is_alive():
                        break

        return

    # '--script 新增作业'
    if options.script:
        if options.crontab:
            # 定时作业
            #解析crontab时间配置参数 分 时 日 月 周 各个取值范围
            res, cron_time = crontab.parse_crontab_time(options.crontab)
            if res != 0:
                print 'crontab时间配置有误:[%s]' % (options.crontab)
                return
            # print cron_time
            db.add_crontab_config()
        else: # 非定时作业
            # 跟随作业
            if options.pre_work_id:
                row_array = db.query_work_config(id=options.pre_work_id)
                row = row_array[0]
                options.start_date = row['start_date']
                options.end_date = row['end_date']
                if options.step != row['step']:
                    options.step = row['step']
                options.force = row['force']
                options.over_notice = row['over_notice']
                options.error_notice = row['error_notice']

            if not options.start_date:
                print '起始日期不能为空'
                return

            # 新增连续作业配置
            print db.insert_work_config(options.script, options.start_date, options.end_date,
                                        assign_hostname, assign_username,
                                        step=options.step, force=options.force,
                                        over_notice=options.over_notice,
                                        error_notice=options.error_notice)


if __name__ == '__main__':
    main()
