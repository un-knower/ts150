#!/usr/bin/python
#coding:utf8

import logging
import logging.config
from var import *

# 日志记录类
class Log:
    """日志记录类"""
    def __init__(self):
        # 日志类定义
        logging.config.fileConfig("logging.conf")
        self.log = logging.getLogger()
        
    def info(self, msg):
        self.log.info(msg)
        pass

    def error(self, msg):
        self.log.error(msg)
        pass

    def debug(self, msg):
        if log_level == 'debug':
            self.log.debug(msg)
        

# 全局变量
log = Log()

def main():
    pass
    # 查询作业ID所有日志
    # 查询作业ID info日志
    # 查询作业ID error日志


if __name__ == '__main__':
    main()