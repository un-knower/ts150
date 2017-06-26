#!/usr/bin/python
#coding:utf8

#############################
#  文件日志模块
#               2017.6 吴招辉
#############################
import os, sys
import logging
import platform

# 日志目录
log_path = "../log"

# 日志级别
log_level = 'debug'
# log_level = 'info'
# log_level = 'error'


def isLinux():
    if platform.system() == 'Linux':
        return True
    else:
        return False


def mkdir(pathName):
    if not os.path.exists(pathName):
        os.mkdir(pathName, 0777)
        if isLinux():
            os.chmod(pathName, 0777)


# 修改文件读写权限，判断是否是本用户的，且需要修改
def chmod(pathName, mode=0777):
    if os.path.exists(pathName) and isLinux():
        stat = os.stat(pathName)
        if stat.st_uid == os.getuid() and stat.st_mode & 0777 != mode:
            os.chmod(pathName, mode)


# 日志记录类
class Log:
    """日志记录类"""
    def __init__(self):
        mkdir(log_path)

        # 日志类定义
        log_level_map = {'debug':logging.DEBUG, 'info':logging.INFO, 'warning':logging.WARNING, 'error':logging.ERROR}
        assert log_level in log_level_map
        self.level = log_level_map[log_level]

        self.log = logging.getLogger()
        self.log.setLevel(self.level)

        if self.level == logging.DEBUG:
            # =====增加debug控制台输出
            console = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter('%(message)s')
            console.setLevel(self.level)
            console.setFormatter(formatter)
            self.log.addHandler(console)

        if self.level <= logging.INFO:
            # =====增加Info 日志文件
            # 创建一个handler，用于写入日志文件
            logFile = '%s/%s.log' % (log_path, 'info')
            fh = logging.FileHandler(logFile)
            chmod(logFile, 0777)

            # 定义handler的输出格式formatter
            formatter = logging.Formatter('%(asctime)s[%(process)d]: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
            fh.setFormatter(formatter)
            fh.setLevel(logging.INFO)
            self.log.addHandler(fh)

        if self.level <= logging.WARNING:
            # 创建一个handler，用于写入日志文件
            logFile = '%s/%s.log' % (log_path, 'warning')
            fh = logging.FileHandler(logFile)
            chmod(logFile, 0777)

            # 定义handler的输出格式formatter
            formatter = logging.Formatter('%(asctime)s[%(process)d]: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
            fh.setFormatter(formatter)
            fh.setLevel(logging.WARNING)
            self.log.addHandler(fh)

        if self.level <= logging.ERROR:
            # =====增加Error 日志文件
            # 创建一个handler，用于写入日志文件
            logFile = '%s/%s.log' % (log_path, 'error')
            fh = logging.FileHandler(logFile)
            chmod(logFile, 0777)

            # 定义handler的输出格式formatter
            formatter = logging.Formatter('%(asctime)s %(filename)s:%(lineno)d %(funcName)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
            fh.setFormatter(formatter)
            fh.setLevel(logging.ERROR)
            self.log.addHandler(fh)

        self.log_map = {}


    def info(self, msg, fileName=None):
        if fileName:
            self.add_log(fileName).info(msg)
        else:
            self.log.info(msg)


    def warning(self, msg, fileName=None):
        if fileName:
            self.add_log(fileName).warning(msg)
        else:
            self.log.warning(msg)


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
        if not fileName:
            return self.log

        fileName = os.path.basename(fileName)
        if fileName not in self.log_map:
            logName = os.path.splitext(fileName)[0]

            logger = logging.getLogger(logName)
            logger.setLevel(self.level)

            # 创建一个handler，用于写入日志文件
            logFile = '%s/log/%s.log' % (run_path, logName)
            fh = logging.FileHandler(logFile)
            chmod(logFile, 0777)

            # 定义handler的输出格式formatter
            formatter = logging.Formatter('%(asctime)s[%(process)d]: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

            fh.setFormatter(formatter)

            logger.setLevel(self.level)
            logger.addHandler(fh)
            self.log_map[fileName] = logger

        return self.log_map[fileName]


# 全局变量
log = Log()


def info(msg, fileName=None):
    log.info(msg, fileName)


def warning(msg, fileName=None):
    log.warning(msg, fileName)


def error(msg, fileName=None):
    log.error(msg, fileName)


def debug(msg, fileName=None):
    log.debug(msg, fileName)


def main():
    log.debug('debug===')
    log.info('Hello', '../train/zz.sh')
    log.info('Hello', '/home/ap/train/zz3.sql')
    log.info('info', 'my.txt')
    log.warning('warning乱码', 'my.txt')
    log.error('errror')
    log.error('errror', '/home/ap/train/zz3.sql')
    log.error('errror', '/home/ap/train/zz3.sql')
    log.error('errror', '/home/ap/train/zz3.sql')



if __name__ == '__main__':
    main()
