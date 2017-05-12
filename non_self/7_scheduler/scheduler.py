#!/usr/bin/python
#coding:utf8

import os
import time
import atexit
from optparse import OptionParser
from multiprocessing import Process
from common_fun import *
from var import *
from log import *
from db import *
import check
import signal 


pidfile = '%s/scheduler_%s.pid' % (run_path, app)


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
        row_map = db.query_work_config(where="where over_date<end_date or (over_date=end_date and status<>'over')")
    rn_list = row_map.keys()
    rn_list.sort()

    for rn in rn_list:
        row = row_map[rn]
        options = eval(row['options'])
        script = options['script']

        # log.debug('找到跑批作业：%s,%s,%s' % (script, row['status'], row['pid']))
        log.debug('找到跑批作业：%s' % (script))

        # 正在处理中，且进程存在，则排除
        if row['status'] in ('processing', 'checking') and exist_pid(row['pid']):
            continue

        log.info('开始运行:id[%d] ts[%s] script[%s] start[%s] end[%s] over[%s] status[%s]' % (row['id'], row['ts'], script, row['start_date'], row['end_date'], row['over_date'], row['status']))

        job = Process(target=run_work_process, args=(db, row), name="%s_%s" % (script, row['start_date']))
        job.start()
        job_list.append(job)

    return job_list


def run_work_process(db, row):
    log.debug("子进程开始执行作业:%s" % row)
    options = eval(row['options'])
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
        log.info('%s[%s][%s]已跑批结束，退出' % (scriptName, start_date, row['end_date']))
        return

    # 按日期一天天往下跑
    for data_date in getDateList(start_date, row['end_date']):
        over_status = run_work_one_date(db, row, data_date)
        if 'over' != over_status:
            break


# 跑一天的脚本
def run_work_one_date(db, row, data_date):
    sched = Scheduler(db=db)
    options = eval(row['options'])
    scriptName = options['script']

    log.debug('准备开始跑脚本:[%s][%s]' % (scriptName, data_date))

    over_status = 'checking'
    db.update_work_config(row['id'], over_date=data_date, status=over_status, pid=os.getpid())


    # 输入依赖检查
    if sched.check_script_depend(row['script'], data_date, io='in', viaTable=not options['force']):
        db.update_work_config(row['id'], over_date=data_date, status='processing', pid=os.getpid())

        log.debug('输入检查通过，开始跑脚本:[%s][%s]' % (scriptName, data_date))
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

        log.debug('跑脚本:[%s][%s]完成:[%s]' % (scriptName, data_date, over_status))
        
        # 输出结果检查
        if over_status == 'over' and not sched.check_script_depend(row['script'], data_date, io='out'):
            over_status = 'non-result'

    else:
        over_status = 'non-depend'
        
    db.update_work_config(row['id'], over_date=data_date, status=over_status, pid=os.getpid())

    return over_status


# 守护进程
def daemon():
    db = DbHelper()

    job_list = []
    while True:
        # 依赖实体检查
        check_job_list = []
        # check_job_list =  (db)
        
        # 运行作业
        run_job_list = run_work(db)

        job_list.extend(check_job_list)
        job_list.extend(run_job_list)

        # 检查进程完成情况
        over_job_list = []
        while True:
            for job in job_list:
                if job in over_job_list:
                    continue

                job.join(1)
                if job.is_alive():
                    # print "pid:%d, name:%s is_alive" % (job.pid, job.name)
                    pass
                else:
                    print "pid:%d, name:%s exit code:%d" % (job.pid, job.name, job.exitcode)
                    over_job_list.append(job)
            if len(over_job_list) >= 1:
                break
        # 删除已完成的作业
        for over_job in over_job_list:
            job_list.remove(over_job)

        # break
        sys.stdout.flush()
        time.sleep(20)
    pass


# 获取脚本语言类型
def get_script_type(scriptName):
    file_array = os.path.splitext(scriptName)
    if len(file_array) == 2:
        ext = file_array[1]
        return ext
    else:
        return None


# 调度类
class Scheduler:
    """调度类"""
    def __init__(self, options=None, scriptName=None, db=None):
        self.options = options
        self.scriptName = scriptName

        self.db = db if db else DbHelper()

        self.depend_key_list = ('IN_CUR_HIVE', 'IN_PRE_HIVE', 'IN_CUR_HDFS', 'IN_CUR_GP', 'IN_CUR_LOCAL',
                                'OUT_CUR_HIVE', 'OUT_CUR_GP', 'OUT_CUR_LOCAL')


    # 插入脚本依赖项到实体表
    def insert_depend_entity(self, script_depend, start_date, end_date):
        for k,entity in script_depend.items():
            depend_condition_array = k.split('_')

            # 检查输入项格式
            assert len(depend_condition_array) == 3

            depend_date = depend_condition_array[1]
            depend_type = depend_condition_array[2]

            # 检查日期判断
            if 'CUR' == depend_date:
                start_date = start_date
            if 'PRE' == depend_date:
                start_date = dateCalc(start_date, -1)

            # valid_success = False
            if 'HDFS' == depend_type:
                pass
                
                # valid_success = check.valid_hdfs_file(entity, check_date)
                
            elif 'HIVE' == depend_type:
                
                if len(entity.split('.')) == 1:
                    entity = '%s.%s' % (default_hive_db, entity)

                # valid_success = check.valid_hive_table(entity, check_date)
            elif 'GP' == depend_type:
                
                if len(entity.split('.')) == 1:
                    entity = '%s.%s' % (default_gp_schema, entity)

                pass
            elif 'LOCAL' == depend_type:
                
                pass
            else:
                continue

            # 按日期一天天增加实体
            for data_date in getDateList(start_date, end_date):
                self.db.insert_entity(depend_type, entity, data_date)


        
    # 获取脚本依赖项
    def get_script_depend(self, scriptName=None):
        if not scriptName:
            scriptName = self.scriptName
        kv_map = {}
        script_type = get_script_type(scriptName)
        if script_type == '.sh':
            kv_map = getRemarkKV(scriptName, None)
        elif script_type == '.py':
            kv_map = getRemarkKV(scriptName, '#')
        elif script_type == '.sql':
            kv_map = getRemarkKV(scriptName, '--')
        else:
            pass

        depend_map = {}
        for key in kv_map.keys():
            if key in self.depend_key_list:
                if '' == kv_map[key]:
                    continue

                depend_map[key] = kv_map[key]
        return depend_map


    # 判断脚本依赖项是否准备好
    def check_script_depend(self, scriptName, data_date, io='in', viaTable=False):
        # 判断脚本输入依赖
        depend_map = self.get_script_depend(scriptName)
        for key in depend_map.keys():
            # 不符合条件的，不检查
            depend_condition_array = key.split('_')

            # 检查输入项格式
            assert len(depend_condition_array) == 3
            
            depend_io = depend_condition_array[0]
            depend_date = depend_condition_array[1]
            depend_type = depend_condition_array[2]

            if 'IN' != depend_io and io == 'in': continue
            if 'OUT' != depend_io and io == 'out': continue
            
            assert depend_date in ('CUR', 'PRE')
            
            # 检查日期判断
            if 'PRE' == depend_date:
                data_date = dateCalc(data_date, -1)

            # print viaTable
            if viaTable:
                # 通过entity表检查
                valid_success = self.check_entity_via_table(depend_type, depend_map[key], data_date)
                if not valid_success:
                    valid_success = self.check_entity(depend_map[key], data_date, depend_type)
            else:
                # 通过实体检查
                valid_success = self.check_entity(depend_map[key], data_date, depend_type)

            # 检查实体不存在，返回False
            if not valid_success:
                log.info('脚本:[%s]对应的实体[%s]:[%s][%s]在日期:[%s]的未完成' % (scriptName, io, key, depend_map[key], data_date))
                return valid_success

        log.debug('脚本:[%s]对应的实体[%s]在日期:[%s]的已完成, 通过' % (scriptName, io, data_date))
        return True


    # 判断实体是否准备好
    def check_entity(self, entity, data_date, flag):
        # 输入项名称或实体类型标志必输一项
        check_date = data_date
        depend_type = flag
        
        assert depend_type in ('HDFS', 'HIVE', 'GP', 'LOCAL')

        self.db.update_entity(depend_type, entity, check_date, 'processing', os.getpid())

        valid_success = False
        if 'HDFS' == depend_type:
            valid_success = check.valid_hdfs_file(entity, check_date)

        elif 'HIVE' == depend_type:
            if len(entity.split('.')) == 1:
                entity = '%s.%s' % (default_hive_db, entity)
            valid_success = check.valid_hive_table(entity, check_date)

        elif 'GP' == depend_type:
            if len(entity.split('.')) == 1:
                entity = '%s.%s' % (default_gp_db, entity)
            pass

        elif 'LOCAL' == depend_type:
            pass

        self.db.update_entity(depend_type, entity, check_date, 'exist' if valid_success else 'not exist', os.getpid())

        # 检查实体不存在，返回False
        if not valid_success:
            return valid_success

        return True


    # 通过表判断实体是否准备好
    def check_entity_via_table(self, flag, entity, data_date):
        where = "where flag='%s' and entity='%s' and data_date='%s'" % \
                (flag, entity, data_date)

        row_map = self.db.query_entity(where=where)
        if len(row_map) < 1:
            log.error('entity表找不到应对实体记录：[%s][%s][%s]' % (flag, entity, data_date))
            return False

        row = row_map.values()[0]

        return True if 'exist' == row['status'] else False


    def run_shell(self, row, data_date):
        options = eval(row['options'])
        scriptName = options['script']

        # 运行脚本
        cmd = '%s -d %s' % (row['script'], data_date)
        (returncode, out_lines) = executeShell_ex(cmd)

        for line in out_lines:
            log.debug(line)

        # 判断脚本输出
        if returncode == 0:
            return 'over'
            
        return 'fail'

    def run_hive_sql(self, row, data_date):
        # options = eval(row['options'])
        script = row['script']

        cmd = 'beeline -f %s ' % script

        patt = re.compile(r'less_(\d+)_date')
        for var in getVar(script):
            # print var
            assert var in ('db', 'log_date', 'p9_data_date', 'data_date', 'less_1_date', 'less_7_date', 'less_30_date', 'less_90_date')
            if var == 'db':
                value = default_hive_db
            elif var in ('log_date', 'p9_data_date', 'data_date'):
                value = data_date
            else:
                m = patt.match(var)
                if m:
                    days = m.group(1)
                    value = dateCalc(data_date, days)
                else:
                    raise CommonError(msg='Hive变量未定义:[%s]' % var)

            cmd += ' --hivevar %s="%s"' % (var, value)

        # 运行脚本
        (returncode, out_lines) = executeShell_ex(cmd)

        for line in out_lines:
            log.debug(line)

        # 判断脚本输出
        if returncode == 0:
            return 'over'
            
        return 'fail'
        

    def run_gp_sql(self, row, data_date):
        pass

    def run_python(self):
        pass

    # 加入新的作业配置与实体依赖
    def add_work_config(self):
        print self.db.insert_work_config(self.scriptName, self.options, self.options.start_date, 
                                         self.options.end_date, self.options.priority)
        script_depend = self.get_script_depend()
        self.insert_depend_entity(script_depend, self.options.start_date, self.options.end_date)


def list_work(*args, **kwargs):
    log.debug('list_work')
    sched = Scheduler()
    row_map = sched.db.query_work_config()

    rn_list = row_map.keys()
    rn_list.sort()

    # 计算最大脚本名称宽度
    max_script_len = 0
    for rn in rn_list:
        row = row_map[rn]
        options = eval(row['options'])
        script = options['script']
        if len(script) > max_script_len:
            max_script_len = len(script)

    log.info('ID  %-23s%s%s %s %s %s    %s' % ('时间戳', '脚本名'.ljust(max_script_len + 4), '起始日期', '终止日期', '完成日期', '处理状态', '进程号'))
    for rn in rn_list:
        row = row_map[rn]
        options = eval(row['options'])
        script = options['script'].ljust(max_script_len + 1)
        log.info('%-4d%-20s%s%-9s%-9s%-9s%-12s%s' % (row['id'], row['ts'], script, row['start_date'], row['end_date'], row['over_date'], row['status'], row['pid']))
        # print row
    exit(0)


def debug_work(*args, **kwargs):
    log.debug('debug_work')
    pid = str(os.getpid())
    file(pidfile, 'w').write('%s' % pid)

    #注册退出函数，根据文件pid判断是否存在进程
    atexit.register(delpid)
    signal.signal(signal.SIGTERM, sig_handler)
    signal.signal(signal.SIGINT, sig_handler)
    signal.signal(signal.SIGQUIT, sig_handler)

    daemon()

    exit(0)


def main():
    parser = OptionParser(usage='usage: %prog [options]')
    parser.add_option("-c", "--script", dest="script", help=u"脚本名称")
    parser.add_option("-s", "--start_date", dest="start_date", help=u"起始日期")
    parser.add_option("-e", "--end_date", dest="end_date", help=u"终止日期，默认与起始日期一致")
    parser.add_option("-f", "--force", action="store_true", default=False, dest="force", help=u"强制重跑")
    parser.add_option("-w", "--no_wait", action="store_true", default=False, dest="no_wait", help=u"源数据未准备好时退出")
    # parser.add_option("-v", "--via_db_check", action="store_true", default=False, dest="via_db_check", help=u"通过表检查完成情况")
    # parser.add_option("-l", "--list", action="store_true", default=False, dest="list", help=u"列出调度任务运行状态")
    parser.add_option("-l", "--list", action="callback", callback=list_work, help=u"列出调度任务运行状态")
    parser.add_option("-t", "--task", type="int", default=0, dest="task_id", help=u"指定任务ID，断点重跑")
    parser.add_option("-p", "--priority", type="int", default=1, dest="priority", help=u"指定任务优先级，1-2")
    parser.add_option("--error_notice", action="store_true", default=False, dest="error_notice", help=u"出错时短信邮件通知")
    parser.add_option("--over_notice", action="store_true", default=False, dest="over_notice", help=u"完成时短信邮件通知")
    parser.add_option("--debug", action="callback", callback=debug_work, help=u"调试模式")
    parser.add_option("--kill", action="callback", callback=stop, help=u"停止调试进程")
    parser.add_option("--deltask", action="store_true", default=False, dest="delete_task", help=u"删除作业配置")

    (options, args) = parser.parse_args()
    sched = Scheduler(options)

    # 指定任务ID跑批
    if options.task_id:
        print '指定任务ID:%d' % options.task_id
        row_map = sched.db.query_work_config(id=options.task_id)

        if len(row_map) == 0:
            print '指定任务ID:%d不存在' % options.task_id
            return

        # 删除作业
        if options.delete_task:
            sched.db.delete_work_config(id=options.task_id)
            return 

        row = row_map[options.task_id]
        status = ''
        # 重新指定起始日期
        if options.start_date:
            assert dateValid(options.start_date)
            start_date = options.start_date
        else:
            # 强制从第一天开始跑
            if options.force:
                start_date = row['start_date']
                status = 'init'
            else:
                start_date = row['over_date']

        # 重新指定终止日期
        if options.end_date:
            assert dateValid(options.end_date)
            end_date = options.end_date
        else:
            end_date = row['end_date']

        # 更新作业配置表
        sched.db.update_work_config(options.task_id, over_date=start_date, status=status, pid=0, end_date=end_date)

        # 开始子进程跑批
        job_list = run_work(sched.db, options.task_id)
        for job in job_list:
            while True:
                job.join(1)
                if not job.is_alive():
                    break

        return

    # '--script 新增作业'
    if options.script:
        # 找到脚本文件
        scriptName = findFile(run_path, options.script)
        if not scriptName:
            print '--script 脚本:[%s]在目录:[%s]未找到' % (run_path, options.script)
            return

        if not options.start_date:
            print '起始日期不能为空'
            return
        if not options.end_date:
            options.end_date = options.start_date

        # 日期格式校验
        if not dateValid(options.start_date) or not dateValid(options.end_date):
            print '日期格式有误:[%s][%s]' % (options.start_date, options.start_date)
            return

        sched.scriptName = scriptName
        sched.add_work_config()


def stop(*args, **kwargs):
    #从pid文件中获取pid
    try:
        pf = file(pidfile,'r')
        pid = int(pf.read().strip())
        pf.close()
    except IOError:
        pid = None
  
    if not pid:   #重启不报错
        message = '进程号记录文件 %s 不存在，服务未开启!\n'
        sys.stderr.write(message % pidfile)
        return

    #杀进程
    try:
        while 1:
            print '杀进程:%d' % pid
            os.kill(pid, signal.SIGTERM)
            time.sleep(0.1)
    except OSError, err:
        err = str(err)
        if err.find('No such process') > 0:
            delpid()
        else:
            print str(err)
            sys.exit(1)


def delpid():
    print '删除进程文件:%s' % pidfile
    if os.path.exists(pidfile):
        os.remove(pidfile)


def sig_handler(sig, frame):
    delpid()
    sys.exit(9)


if __name__ == '__main__':
    main()

