#!/usr/bin/python
#coding:utf8

# 按天连续作业配置

import os
import time
from optparse import OptionParser
from multiprocessing import Process
import dbScheduler
from var import *
from scheduler import *
from baseFun import *
sys.path.append("../python_common/")
import log
from common_fun import *


# 运行作业
def run_work(id=None):
    log.debug('运行跑批作业调度')

    db = dbScheduler.DbScheduler(remote_mode=remote_mode)

    job_list = []
    if id:
        row_array = db.query_work_config(id=id)
    else:
        where = "where (over_date<end_date or (over_date=end_date and status<>'over')) " + \
                "  and hostname='%s' and username='%s'" % (hostname, username)
        row_array = db.query_work_config(where=where)

    for row in row_array:
        script = row['base_script']

        # log.debug('找到跑批作业：%s,%s,%s' % (script, row['status'], row['pid']))
        log.info('找到跑批作业：%s' % (script), script)

        # 正在处理中，且进程存在，则排除
        if row['status'] in ('processing', 'checking') and exist_pid(row['pid']):
            log.info('作业：%s正在处理中，跳过' % (script), script)
            continue

        # 作业暂停，则排除
        if row['next_action'] in ('pause', 'local_run'):
            log.info('作业：%s暂停:[%s]，跳过' % (script, row['next_action']), script)
            db.update_work_config(row['id'], status=row['next_action'])
            continue

        # 作业日期到今天或昨天，检查时间延长到1小时1次
        if row['over_date'] == '':
            start_date = row['start_date']
        else:
            start_date = dateCalc(row['over_date'], row['step'])

        yesterday = dateCalc(today(), -1)
        if start_date in (today(), yesterday) or start_date > today():
            minuteDelta = timeCalc(row['ts'], 'minute')
            if minuteDelta < 120:
                log.info('作业：%s检查时间延长到2小时，跳过' % (script), script)
                continue

        log.info('开始运行子进程:id[%d] script[%s] start[%s] end[%s] over[%s] status[%s]' % (
            row['id'], script, row['start_date'], row['end_date'], row['over_date'], row['status']), script)

        job = Process(target=run_work_process, args=(row, ), name="%s_%s" % (script, row['start_date']))
        job.start()
        job_list.append(job)

    return job_list


# 运行从起始日期到终止日期作业的子进程
def run_work_process(row):
    log.debug("子进程开始执行作业:%s" % row)
    script = row['base_script']

    # 断点重跑
    if row['over_date'] == '':
        start_date = row['start_date']
    else:
        start_date = dateCalc(row['over_date'], row['step'])

    if start_date > row['end_date']:
        log.info('%s[%s][%s]已跑批结束，退出' % (script, start_date, row['end_date']), script)
        return

    log.info('%s[%s][%s]开始跑批' % (script, start_date, row['end_date']), script)
    # 按日期一天天往下跑
    over_status = None
    for data_date in getDateList(start_date, row['end_date'], row['step']):
        over_status = run_work_one_date(row, data_date)
        if 'over' != over_status:
            break

    notice(row, over_status,
           '连续跑批脚本执行完成[%s][%s][%s]' % (script, start_date, row['end_date']),
           '连续跑批脚本执行失败[%s][%s][%s]' % (script, start_date, row['end_date']),
           logFile=script)


# 跑一天的脚本
def run_work_one_date(row, data_date):
    sched = Scheduler()
    script = row['base_script']
    db = sched.db

    log.info('准备开始跑脚本:[%s]处理[%s]的数据' % (script, data_date), script)

    over_status = 'checking'
    db.update_work_config(row['id'], process_date=data_date, status=over_status, pid=os.getpid())

    over_date = None
    # 输入依赖检查
    if sched.check_script_depend(row['script'], data_date, io='in', viaTable=not row['force']):
        db.update_work_config(row['id'], process_date=data_date, status='processing', pid=os.getpid())

        log.info('输入检查通过，开始跑脚本:[%s][%s]' % (script, data_date), script)

        # 运行脚本
        over_status = sched.run(row, data_date)

        log.info('跑脚本:[%s][%s]完成:[%s]' % (script, data_date, over_status), script)

        # 输出结果检查
        if over_status == 'over': 
            if sched.check_script_depend(row['script'], data_date, io='out'):
                over_date = data_date
            else:
                over_status = 'non-result'
    else:
        over_status = 'non-depend'

    db.update_work_config(row['id'], process_date=data_date, over_date=over_date, status=over_status, pid=0)

    return over_status


# 连续作业守护进程
def daemon():
    job_list = []
    while True:
        # 运行作业
        run_job_list = run_work()
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
        time.sleep(120)


def list_work_all(*args, **kwargs):
    list_work(detail=True)


def list_work_processing(*args, **kwargs):
    list_work(where="where status<>'over'")

def list_work_my(*args, **kwargs):
    list_work(where="where status<>'over' and hostname='%s' and username='%s'" %(hostname, username))


def list_work(where='', detail=False):
    db = dbScheduler.DbScheduler(remote_mode=remote_mode)
    row_array = db.query_work_config(where=where)
    for row in row_array:
        # 非本节点运行的进程，不检查进程是否存在
        if row['hostname'] == hostname:
            pid_str = '存在' if row['pid'] and exist_pid(row['pid']) else ''
        else:
            pid_str = ''
        row['pid_exist'] = pid_str
        row['ts_short'] = row['ts'][11:]
        row['force'] = 'Force' if row['force'] else ''
        row['over_notice'] = 'over_notice' if row['over_notice'] else ''
        row['error_notice'] = 'error_notice' if row['error_notice'] else ''

    if detail:
        title_list = ['id', 'ts', 'base_script',
                      'start_date', 'end_date', 'hostname', 'username',
                      'step', 'force', 'over_notice', 'error_notice',
                      'process_date', 'over_date', 'status', 'pid', 'pid_exist',
                      'next_action', 'pre_work_id']
    else:
        title_list = ['id', 'ts_short', 'base_script',
                      'start_date', 'end_date', 'hostname', 'username',
                      'step', 'process_date', 'over_date', 'status',
                      'pid', 'pid_exist', 'next_action', 'pre_work_id']

    display_table(row_array, title_list)


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
    parser.add_option("--listmy", action="callback", callback=list_work_my, help=u"列出调度任务运行状态")
    parser.add_option("--debug", action="callback", callback=debug_work, help=u"调试模式")

    (options, args) = parser.parse_args()

    db = dbScheduler.DbScheduler(remote_mode=remote_mode)

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
        next_action = None

        # 删除作业
        if options.delete_task:
            db.delete_work_config(id=options.task_id)
            return

        # 暂停作业
        if options.pause_task:
            next_action = 'pause'

        # 恢复作业
        if options.resume_task:
            next_action = 'resume'

        # 调试作业
        if options.run_task:
            next_action = 'local_run'

        # 重新指定终止日期
        end_date = None
        if options.end_date:
            end_date = options.end_date

        row = db.query_work_config(id=options.task_id)[0]
        # 改为强制模式
        force = None
        if row['force'] != int(options.force):
            force = options.force

        error_notice = None
        if row['error_notice'] != int(options.error_notice):
            error_notice = options.error_notice

        over_notice = None
        if row['over_notice'] != int(options.over_notice):
            over_notice = options.over_notice

        # 更新作业配置表
        db.update_work_config(options.task_id, force=force,
                              over_notice=over_notice, error_notice=error_notice,
                              end_date=end_date, next_action=next_action)

        # 调试作业
        if options.run_task:
            # 开始子进程跑批
            job_list = run_work(options.task_id)
            for job in job_list:
                while True:
                    job.join(1)
                    if not job.is_alive():
                        break

        return

    # '--script 新增作业'
    if options.script:
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
