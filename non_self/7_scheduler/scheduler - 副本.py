#!/usr/bin/python
#coding:utf8

import os
import time
import atexit
from optparse import OptionParser
from multiprocessing import Process, Pool
from common_fun import *
from var import *
from log import *
from db import *
import check
import signal 
import scheduler

pidfile = '%s/scheduler_%s.pid' % (run_path, app)

# 子进程执行函数不能在类中
def check_depend_entity_callback(arg):
    # db = DbHelper()
    (db, id, over_status) = arg
    if over_status:
        db.update_entity(id, 'exist')
    else:
        db.update_entity(id, 'not exist')

    return arg


def check_depend_entity_start(db, row):
    # db = DbHelper()
    db.update_entity(row['id'], 'processing')
    over_status = False
    if row['flag'] == '1':
        #HDFS
        path = '%s/%s' % (row['entity'], row['data_date'])
        over_status = check.has_hdfs_file(path)
        # pool.apply_async(func=check.has_hdfs_file, args=(path,), callback=self.Bar)

    elif row['flag'] == '2':
        #Hive
        # pool.apply_async(func=check.has_hive_table_partition, args=(row['entity'], row['data_date']), callback=self.Bar)
        over_status = check.has_hive_table_partition(row['entity'], row['data_date'])
        pass
    elif row['flag'] == '3':
        #GP
        pass
    elif row['flag'] == '4':
        #local file
        pass

    return (db, row['id'], over_status)

# 依赖项实体表--数据检查
def check_depend_entity(db, id=0):
    job_list = []
    row_map = {}
    if id > 0:
        row_map = db.query_entity(id)
    else:
        # 时间差大于15分钟
        minute_interval = "julianday('now', 'localtime')*1440 - julianday(ts)*1440 > 15"
        where="status<>'exist' and (status<>'processing' and %s) " % minute_interval
        sql = "select flag, entity, min(data_date) as min_data_date from entity where %s group by flag, entity" % where
        row_array = db.query_sql(sql)

        for row in row_array:
            again_row_map = db.query_entity(where="where flag='%s' and entity='%s' and data_date='%s'" % \
                            (row['flag'], row['entity'], row['min_data_date']))

            row_map.update(again_row_map)

    for row in row_map.values():
        # print row
        job = check_pool.apply_async(func=check_depend_entity_start, args=(db, row), callback=check_depend_entity_callback)
        # job = check_pool.apply_async(func=scheduler.check_depend_entity_start, args=(db, row), callback=scheduler.check_depend_entity_callback)
        job_list.append(job)

    return job_list


# 运行作业
def run_work(db):
    job_list = []
    row_map = db.query_work_config(where="where (end_date > over_date or over_date is null) and status is null")
    rn_list = row_map.keys()
    rn_list.sort()

    for rn in rn_list:
        row = row_map[rn]
        options = eval(row['options'])
        script = options['script']
        log.info('id[%d] ts[%s] script[%s] start[%s] end[%s] over[%s] status[%s]' % (row['id'], row['ts'], script, row['start_date'], row['end_date'], row['over_date'], row['status']))


        job = run_pool.apply_async(func=scheduler.run_work_start, args=(db, row), callback=scheduler.run_work_callback)
        job_list.append(job)

    return job_list

def run_work_callback(arg):
    print arg
    # (db, id, over_status) = arg
    # if over_status:
    #     db.update_entity(id, 'exist')
    # else:
    #     db.update_entity(id, 'not exist')

    # return arg


def run_work_start(db, row):
    log.debug("子进程开始执行作业:%s", row)
    time.sleep(10)
    # db.update_entity(row['id'], 'processing')
    over_status = False
    # if row['flag'] == '1':
    #     #HDFS
    #     path = '%s/%s' % (row['entity'], row['data_date'])
    #     over_status = check.has_hdfs_file(path)
    #     # pool.apply_async(func=check.has_hdfs_file, args=(path,), callback=self.Bar)

    # elif row['flag'] == '2':
    #     #Hive
    #     # pool.apply_async(func=check.has_hive_table_partition, args=(row['entity'], row['data_date']), callback=self.Bar)
    #     over_status = check.has_hive_table_partition(row['entity'], row['data_date'])
    #     pass
    # elif row['flag'] == '3':
    #     #GP
    #     pass
    # elif row['flag'] == '4':
    #     #local file
    #     pass

    return (db, row['id'], over_status)

# 守护进程
def daemon():
    db = DbHelper()

    while True:
        # 依赖实体检查
        check_job_list = check_depend_entity(db)
        
        # 运行作业
        # run_job_list = run_work(db)

        check_pool.close()
        # run_pool.close()

        check_pool.join()
        # run_pool.join()

        break
        sys.stdout.flush()
        time.sleep(20)
    pass


# 调度类
class Scheduler:
    """调度类"""
    def __init__(self, options=None, scriptName=None):
        self.options = options
        self.scriptName = scriptName

        self.db = DbHelper()
        self.depend_key_list = ('IN_CUR_HIVE', 'IN_PRE_HIVE', 'IN_CUR_HDFS', 'OUT_CUR_HIVE')

    # 获取脚本语言类型
    def get_script_type(self, scriptName=None):
        if scriptName:
            script = scriptName
        else:
            script = self.scriptName
        file_array = os.path.splitext(script)
        if len(file_array) == 2:
            ext = file_array[1]
            return ext
        else:
            return None

    # 脚本依赖项
    def get_script_depend(self, scriptName=None):
        if not scriptName:
            scriptName = self.scriptName
        kv_map = {}
        script_type = self.get_script_type(scriptName)
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
                depend_map[key] = kv_map[key]
        return depend_map


    # 插入脚本依赖项到实体表
    def insert_depend_entity(self, script_depend, start_date, end_date):
        for k,v in script_depend.items():
            if 'HDFS' in k:
                flag = '1'
            elif 'HIVE' in k:
                flag = '2'
            elif 'GP' in k:
                flag = '3'
            else:
                continue
            
            entity_list = v.split(' ')
            for entity in entity_list:
                data_date = start_date
                while data_date <= end_date:
                    if flag == '2' and len(entity.split('.')) == 1:
                        entity = '%s.%s' % (default_hive_db, entity)
                    self.db.insert_entity(flag, entity, data_date)
                    data_date = dateCalc(data_date, 1)

        

    def run_shell(self):
        pass

    def run_hive_sql(self):
        pass

    def run_gp_sql(self):
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

    log.info('ID  %-23s%s%s %s %s %s' % ('时间戳', '脚本名'.ljust(max_script_len + 4), '起始日期', '终止日期', '完成日期', '处理状态'))
    for rn in rn_list:
        row = row_map[rn]
        options = eval(row['options'])
        script = options['script'].ljust(max_script_len + 1)
        log.info('%-4d%-20s%s%-9s%-9s%-9s%s' % (row['id'], row['ts'], script, row['start_date'], row['end_date'], row['over_date'], row['status']))
        # print row
    exit(0)

def debug_work(*args, **kwargs):
    log.debug('debug_work')

    #注册退出函数，根据文件pid判断是否存在进程
    atexit.register(delpid)

    # sched = Scheduler()
    # sched.daemon()
    daemon()
    exit(0)

def main():
    parser = OptionParser(usage='usage: %prog [options]')
    parser.add_option("-c", "--script", dest="script", help=u"脚本名称")
    parser.add_option("-s", "--start_date", dest="start_date", help=u"起始日期")
    parser.add_option("-e", "--end_date", dest="end_date", help=u"终止日期，默认与起始日期一致")
    parser.add_option("-f", "--force", action="store_true", default=False, dest="force", help=u"强制重跑")
    parser.add_option("-w", "--no_wait", action="store_true", default=False, dest="no_wait", help=u"源数据未准备好时退出")
    parser.add_option("-v", "--via_db_check", action="store_true", default=True, dest="via_db_check", help=u"通过Task任务表检查完成情况")
    # parser.add_option("-l", "--list", action="store_true", default=False, dest="list", help=u"列出调度任务运行状态")
    parser.add_option("-l", "--list", action="callback", callback=list_work, help=u"列出调度任务运行状态")
    parser.add_option("-t", "--task", type="int", default=0, dest="task_id", help=u"指定任务ID，断点重跑")
    parser.add_option("-p", "--priority", type="int", default=1, dest="priority", help=u"指定任务优先级，1-2")
    parser.add_option("--error_notice", action="store_true", default=False, dest="error_notice", help=u"出错时短信邮件通知")
    parser.add_option("--over_notice", action="store_true", default=False, dest="over_notice", help=u"完成时短信邮件通知")
    parser.add_option("--debug", action="callback", callback=debug_work, help=u"调试模式")
    parser.add_option("--kill", action="callback", callback=stop, help=u"停止调试进程")


    (options, args) = parser.parse_args()

    if options.task_id:
        print '指定任务ID:%d，断点重跑' % options.task_id
        return

    if not options.script:
        print '--script 脚本名称不能为空'
        return

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

    sched = Scheduler(options, scriptName)
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
            os.kill(pid, signal.SIGKILL)
            time.sleep(0.1)
            #os.system('hadoop-daemon.sh stop datanode')
            #os.system('hadoop-daemon.sh stop tasktracker')
            #os.remove(self.pidfile)
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
    # check_pool.close()
    # check_pool.terminate()
    delpid()

    sys.exit(9)


if __name__ == '__main__':


    signal.signal(signal.SIGTERM, sig_handler)
    signal.signal(signal.SIGINT, sig_handler)
    signal.signal(signal.SIGQUIT, sig_handler)

    pid = str(os.getpid())
    file(pidfile, 'w').write('%s' % pid)

    # 初始化进程池
    check_pool = Pool(5)
    run_pool = Pool(5)

    main()
