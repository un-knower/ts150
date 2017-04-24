#!/usr/bin/python
#coding:utf8

import os
import time
from optparse import OptionParser
from multiprocessing import Process
from common_fun import *
from var import *
from log import *


# 调度类
class Scheduler:
    """调度类"""
    def __init__(self, arg=""):
        self.arg = arg
        
    def run_shell(self):
        pass

    def run_hive_sql(self):
        pass

    def run_gp_sql(self):
        pass

    def domain(self):
        pass


def list_work(*args, **kwargs):
    print 'list_work'
    exit(0)

def start(*args, **kwargs):
    '''启动服务'''
    print 'start: %d' % os.getpid()
    
    exit(0)

def stop(*args, **kwargs):
    print 'stop'
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
    parser.add_option("--error_notice", action="store_true", default=False, dest="error_notice", help=u"出错时短信邮件通知")
    parser.add_option("--over_notice", action="store_true", default=False, dest="over_notice", help=u"完成时短信邮件通知")
    parser.add_option("--debug", action="store_true", default=False, dest="debug", help=u"调试模式")
    parser.add_option("--start", action="callback", callback=start, help=u"启动后台服务")
    parser.add_option("--stop", action="callback", callback=stop, help=u"关闭后台服务")

    
    (options, args) = parser.parse_args()

    print options
    # if options.delete_dirs != "":
    #     for delete_dir in options.delete_dirs.split(' '):
    #         clear(delete_dir, options)
    # else:
    #     basePath = '/home/ap/dip/file/archive/input/ts150/000000000'
    #     clear('%s' % basePath, options)

if __name__ == '__main__':
    main()
