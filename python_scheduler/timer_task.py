#!/usr/bin/python
#coding:utf8

# 定时作业配置

import os
import time
from optparse import OptionParser
from multiprocessing import Process
import dbScheduler
from var import *
from scheduler import *
from baseFun import *
import crontab
sys.path.append("../python_common/")
import log
from common_fun import *


# 跑定时脚本
def run_crontab_process(db, row, data_date):
    sched = Scheduler()
    script = row['base_script']

    log.info('准备开始跑脚本:[%s]处理[%s]的数据' % (script, data_date), script)

    over_status = 'checking'
    db.save_crontab_config(id=row['id'], status=over_status, pid=os.getpid())

    # 输入依赖检查
    if sched.check_script_depend(row['script'], data_date, io='in'):
        db.save_crontab_config(id=row['id'], status='processing', pid=os.getpid())

        log.info('输入检查通过，开始跑脚本:[%s][%s]' % (script, data_date), script)

        # 运行脚本
        over_status = sched.run(row, data_date)

        log.info('跑脚本:[%s][%s]完成:[%s]' % (script, data_date, over_status), script)

        # 输出结果检查
        if over_status == 'over' and not sched.check_script_depend(row['script'], data_date, io='out'):
            over_status = 'non-result'

    else:
        over_status = 'non-depend'

    db.save_crontab_config(id=row['id'], status=over_status, pid=0)

    notice(row, over_status,
           '定时脚本检查通过[%s][%s]' % (script, data_date),
           '定时脚本检查不通过[%s][%s]' % (script, data_date),
           logFile=script)

    return over_status


# 定时任务守护进程
def daemon():
    db = dbScheduler.DbScheduler(remote_mode=remote_mode)

    job_list = []
    while True:
        # 查找所有不是正在跑的脚本
        where = "where hostname='%s' and username='%s'" % (hostname, username)
        row_array = db.query_crontab_config(where=where)

        for row in row_array:
            script = row['base_script']

            # 正在处理中，且进程存在，则排除
            if row['status'] in ('processing', 'checking') and exist_pid(row['pid']):
                log.info('作业：%s正在处理中，跳过' % (script), script)
                continue

            # 作业暂停，则排除
            if row['next_action'] in ('pause', 'local_run'):
                log.info('作业：%s暂停:[%s]，跳过' % (script, row['next_action']), script)
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
    db = dbScheduler.DbScheduler(remote_mode=remote_mode)
    row_array = db.query_crontab_config(where=where)
    for row in row_array:
        # 非本节点运行的进程，不检查进程是否存在
        if row['hostname'] == hostname:
            pid_str = 'Has' if row['pid'] and exist_pid(row['pid']) else 'No'
        else:
            pid_str = ''
        row['pid_exist'] = pid_str
        row['ts_short'] = row['ts'][11:]
        row['error_notice'] = 'error_notice' if row['error_notice'] else ''

    if detail:
        title_list = ['id', 'ts', 'crontab', 'base_script',
                      'hostname', 'username', 'error_notice',
                      'status', 'pid', 'pid_exist', 'next_action']
    else:
        title_list = ['id', 'ts_short', 'crontab', 'base_script',
                      'hostname', 'username', 'status',
                      'pid', 'pid_exist', 'next_action']

    display_table(row_array, title_list)


def debug_work(*args, **kwargs):
    log.debug('debug_work')
    timer()
    exit(0)


# 校验并调整输入格式
def valid(options, db):
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

    if options.task_id:
        row_array = db.query_crontab_config(id=options.task_id)

        if len(row_array) == 0:
            print '指定任务ID:%d不存在' % task_id
            return False

    return True


def main():
    parser = OptionParser(usage='usage: %prog [options]')
    parser.add_option("-c", "--script", dest="script", help=u"脚本名称")
    parser.add_option("-r", "--crontab", dest="crontab", help=u"crontab定时配置字符串")
    parser.add_option("-t", "--task", type="int", default=0, dest="task_id", help=u"指定任务ID，断点重跑")
    parser.add_option("-u", "--username", dest="username", help=u"指定运行任务的主机与用户 hostname.username")
    parser.add_option("--error_notice", action="store_true", default=False, dest="error_notice", help=u"出错时短信邮件通知")
    parser.add_option("--deltask", action="store_true", default=False, dest="delete_task", help=u"删除作业配置")
    parser.add_option("--pause", action="store_true", default=False, dest="pause_task", help=u"暂停作业执行")
    parser.add_option("--resume", action="store_true", default=False, dest="resume_task", help=u"恢复作业执行")

    parser.add_option("-l", "--list", action="callback", callback=list_work_processing, help=u"列出调度任务运行状态")
    parser.add_option("--la", action="callback", callback=list_work_all, help=u"列出调度任务运行状态")
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
            db.delete_crontab_config(id=options.task_id)
            return

        # 暂停作业
        if options.pause_task:
            next_action = 'pause'

        # 恢复作业
        if options.resume_task:
            next_action = 'resume'

        row = db.query_crontab_config(id=options.task_id)[0]
        # 改为强制模式
        force = None
        if row['force'] != int(options.force):
            force = options.force

        error_notice = None
        if row['error_notice'] != int(options.error_notice):
            error_notice = options.error_notice

        # 更新作业配置表
        db.save_crontab_config(options.task_id, force=force,
                               error_notice=error_notice,
                               next_action=next_action)

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

            # 新增作业配置
            print db.save_crontab_config(crontab=options.crontab, script=options.script,
                                         hostname=assign_hostname, username=assign_username,
                                         error_notice=options.error_notice)


if __name__ == '__main__':
    main()
