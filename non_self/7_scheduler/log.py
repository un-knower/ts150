#!/usr/bin/python
#coding:utf8

import os, sys
import logging
import logging.config
from var import *


# 日志记录类
class Log:
    """日志记录类"""
    def __init__(self):

        log_path = '%s/log' % run_path
        if not os.path.exists(log_path):
            os.mkdir(log_path, 0777)

        # 日志类定义
        # logging.config.fileConfig("logging.conf")
        self.log = logging.getLogger()

        if log_level == 'debug':
            self.level = logging.DEBUG
            # =====增加debug控制台输出
            # print help(logging.StreamHandler)
            console = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter('%(message)s')
            console.setLevel(logging.DEBUG)
            console.setFormatter(formatter)
            self.log.addHandler(console)

        elif log_level == 'info':
            self.level = logging.INFO
            # =====增加Info 日志文件
            # 创建一个handler，用于写入日志文件    
            fh = logging.FileHandler('%s/%s.log' % (log_path, 'info'))
            # 定义handler的输出格式formatter    
            formatter = logging.Formatter('%(asctime)s[%(process)d]: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')  
            fh.setFormatter(formatter)
            fh.setLevel(logging.INFO)
            self.log.addHandler(fh)
        else:
            self.level = logging.ERROR

        # =====增加Error 日志文件
        # 创建一个handler，用于写入日志文件    
        fh = logging.FileHandler('%s/%s.log' % (log_path, 'error'))
        # 定义handler的输出格式formatter    
        formatter = logging.Formatter('%(asctime)s %(filename)s:%(lineno)d %(funcName)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')  
        fh.setFormatter(formatter)
        fh.setLevel(logging.ERROR)
        self.log.addHandler(fh)

        self.log.setLevel(self.level)
        self.log_map = {}
        

    def info(self, msg, fileName=None):
        if fileName:
            self.add_log(fileName).info(msg)
        else:
            self.log.info(msg)


    def error(self, msg, fileName=None):
        if fileName:
            self.add_log(fileName).error(msg)
        else:
            self.log.error(msg)


    def debug(self, msg, fileName=None):
        if fileName:
            self.add_log(fileName).debug(msg)
        else:
            self.log.debug(msg)


    def add_log(self, fileName):
        if fileName not in self.log_map:
            fileName = os.path.basename(fileName)
            logName = os.path.splitext(fileName)[0]

            logger = logging.getLogger(logName)
            logger.setLevel(self.level)

            # 创建一个handler，用于写入日志文件    
            fh = logging.FileHandler('%s/log/%s.log' % (run_path, logName))

            # 定义handler的输出格式formatter    
            formatter = logging.Formatter('%(asctime)s[%(process)d]: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')  

            fh.setFormatter(formatter)  

            logger.setLevel(self.level)
            logger.addHandler(fh)
            self.log_map[fileName] = logger

        return self.log_map[fileName]

 
# 全局变量
log = Log()


def main():
    log.debug('debug===')
    log.info('Hello', '../train/zz.sh')
    log.info('Hello', '/home/ap/train/zz3.sql')
    log.error('errror', '/home/ap/train/zz3.sql')
    # 查询作业ID所有日志
    # 查询作业ID info日志
    # 查询作业ID error日志


if __name__ == '__main__':
    main()
