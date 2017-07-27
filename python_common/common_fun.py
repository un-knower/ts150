#!/usr/bin/env python
#coding: utf-8

'''
    常用函数库           2016.2.27
'''

import sys, os, time, datetime, re
import platform
import socket
import shlex
import commands
import subprocess
import getpass
import select
import log

# 取主机名
hostname = socket.gethostname()
# print hostname
# 取用户名
username = getpass.getuser()
# print username


def today(fmt='%Y%m%d'):
    '''取当前日期字符串YYYYMMDD'''
    t = time.strftime(fmt, time.localtime())
    return t


def dateCalc(strDate, iDay, fmt='%Y%m%d'):
    ''' 日期运算 '''
    chkDate = time.strptime(strDate, fmt)
    dateTmp = datetime.datetime(chkDate[0], chkDate[1], chkDate[2])
    resultDate = dateTmp + datetime.timedelta(days=iDay)
    return datetime.date.strftime(resultDate, fmt)


def dateValid(strDate, fmt='%Y%m%d'):
    ''' 日期格式检验 '''
    try:
        chkDate = time.strptime(strDate, fmt)
        # print chkDate
        return True
    except Exception as e:
        return False


# 计算指定时间戳与当前时间差
def timeCalc(ts_str, ret_type='second'):
    if len(ts_str) == 8:
        fmt = '%Y%m%d'
    if len(ts_str) == 10:
        fmt = '%Y-%m-%d'
    if len(ts_str) == 19:
        fmt = '%Y-%m-%d %H:%M:%S'

    ts = datetime.datetime.strptime(ts_str, fmt)

    deltaDay = (datetime.datetime.now() - ts).days
    deltaSecond = (datetime.datetime.now() - ts).seconds
    if deltaDay != 0:
        deltaSecond += deltaDay * 86400

    if ret_type == 'day':
        return deltaDay

    if ret_type == 'week':
        return deltaDay / 7

    if ret_type == 'month':
        return deltaDay / 30

    if ret_type == 'second':
        return deltaSecond

    if ret_type == 'minute':
        return deltaSecond / 60

    if ret_type == 'hour':
        return deltaSecond / 3600


def getDateList(start_date, end_date, step=1):
    ''' 返回两个日期间每一天的列表 '''
    if not dateValid(start_date):
        return None

    if not dateValid(end_date):
        return None

    ret_list = []
    # 防止Start - end 写反
    if start_date > end_date:
        max_date = start_date
        min_date = end_date
    else:
        min_date = start_date
        max_date = end_date

    data_date = min_date
    while data_date <= max_date:
        ret_list.append(data_date)
        data_date = dateCalc(data_date, step)

    return ret_list


def exist_pid(pid):
    if isinstance(pid, str):
        cmd = 'ps -p %s' % pid
    elif isinstance(pid, int):
        cmd = 'ps -p %d' % pid
    else:
        return False

    # print cmd
    returncode, out_lines = executeShell(cmd)
    if returncode == 0:
        return True
    else:
        return False


def executeShell(cmdstring):
    returncode, out = commands.getstatusoutput(cmdstring)
    out_lines = out.split('\n')
    # print (returncode, out_lines)
    # for line in out_lines:
    #     print type(line)
    return (returncode, out_lines)


# 可靠的Shell
def executeShell_ex(cmdstring, num=3):
    for i in range(num):
        #(returncode, out_lines, err_lines) = execute_command(cmdstring)
        (returncode, out_lines) = executeShell(cmdstring)

        if returncode != 0:
            continue

        return (returncode, out_lines)

    # for line in out_lines:
    #     print line
    raise CommonError(msg='命令出错:%s\n%s' % (cmdstring, '\n'.join(out_lines)))


def execute_command(cmdstring, cwd=None, timeout=None, logFile='execute_command'):
    """执行一个SHELL命令
            封装了subprocess的Popen方法, 支持超时判断，支持读取stdout和stderr
           参数:
        cwd: 运行命令时更改路径，如果被设定，子进程会直接先更改当前路径到cwd
        timeout: 超时时间，秒，支持小数，精度0.1秒
        shell: 是否通过shell运行
    Returns: return_code
    Raises:  Exception: 执行超时
    """
    cmdstring_list = shlex.split(cmdstring)
    if timeout:
        end_time = datetime.datetime.now() + datetime.timedelta(seconds=timeout)

    #没有指定标准输出和错误输出的管道，因此会打印到屏幕上；
    sub = subprocess.Popen(cmdstring_list, cwd=cwd,
                           stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE, shell=False, bufsize=1024)

    read_list = [sub.stdout, sub.stderr, ]
    write_list = []
    except_list = []
    out_lines = []
    err_lines = []
    #subprocess.poll()方法：检查子进程是否结束了，如果结束了，设定并返回码，放在subprocess.returncode变量中
    while sub.poll() is None:
        readable, writeable, exceptional = select.select(read_list, write_list, except_list, 0.1)
        for r in readable:
            line = r.readline().strip()
            if len(line) == 0: continue

            if r is sub.stdout:
                # print 'stdout:[%s]' % (line)
                log.info(line, logFile)
                out_lines.append(line)
            if r is sub.stderr:
                log.info(line, logFile)
                # print 'stderr:[%s]' % (line)
                err_lines.append(line)

        if timeout:
            if end_time <= datetime.datetime.now():
                # raise Exception("Timeout：%s" % cmdstring)
                return (1, [], ['timeout'])

    remainder_out_lines = sub.stdout.readlines()
    remainder_err_lines = sub.stderr.readlines()
    for line in remainder_out_lines:
        line = line.strip()
        out_lines.append(line)
        log.info(line, logFile)

    for line in remainder_err_lines:
        line = line.strip()
        err_lines.append(line)
        log.info(line, logFile)

    returncode = sub.returncode

    return (returncode, out_lines, err_lines)


class CommonError(Exception):
    """公共异常类"""
    def __init__(self, code="", msg=""):
        self.code = code
        self.msg = msg
        sys.stderr.write('异常: %s (%s)\n' % (code, msg))
        # sys.stderr.write('Error: %s (%s)\n' % (code, msg))


    def __repr__(self):
        return '自定义出错信息%s:%s' % self.code, self.msg


def isLinux():
    # print platform.system()
    if platform.system() == 'Linux':
        return True
    else:
        return False


def findFile(path, findFileName):
    ''' 查找文件 '''
    if not os.path.exists(path):
        return None

    # 全路径文件不需要查找
    if '/' == findFileName[0]:
        if os.path.exists(findFileName):
            return findFileName
        else:
            return None

    for root, dirs, files in os.walk(path):
        # root为文件夹
        # stat = os.stat(root)

        if root != path:
            pass
        for fileName in files:
            fullFileName = root + os.sep + fileName
            if fileName == findFileName:
                return fullFileName
    return None


# 获取脚本语言类型
def get_script_type(scriptName):
    file_array = os.path.splitext(scriptName)
    if len(file_array) == 2:
        ext = file_array[1]
        return ext
    else:
        return None


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


def get_ip():
    cmd = "/sbin/ifconfig | grep 'inet addr:' | grep -v '127.0.0.1' | awk  '{print $2}' | awk -F: '{print $2}'"

    returncode, ip_list = executeShell(cmd)

    return ip_list


def dns2ip(dns):
    ip = ''
    cmd = 'ping -c 1 %s' % dns
    returncode, lines = executeShell(cmd)
    if returncode != 0:
        print '%s 域名解释出错' % cmd
    else:
        patt = re.compile(r'.*\((\d+\.\d+\.\d+\.\d+)\)')
        m = patt.match(lines[0])
        if m:
            ip = m.group(1)
            # print '%s   =====>   %s' % (dns, ip)
    return ip


# 判断变量是否已存在
def isset(v):
    try:
        type(eval(v))
    except:
        return False
    else:
        return True


def is_chinese(uchar):
    """判断一个unicode是否是汉字"""
    value = ord(uchar)

    # 汉字
    if value >= 0x4e00 and value <= 0x9fa5:
        return True
    else:
        # 全角空格
        if value == 0x3000:
            return True

        # 除了空格其他的全角半角的公式为:半角=全角-0xfee0
        if value > 0x0020 + 0xfee0 and value < 0x7e + 0xfee0:
            return True

        return False


# 返回中英文混合字符串显示长度
def char_length(in_str, coding='utf8'):
    if isinstance(in_str, unicode):
        string = in_str
    else:
        string = in_str.decode(coding)

    length = 0
    for c in string:
        if is_chinese(c):
            length += 2
        else:
            length += 1

    return length


# 中英文混合字符串按长度补空格
def char_ljust(in_str, display_length, coding='utf8'):
    length = char_length(in_str, coding)

    return in_str + ''.ljust(display_length - length)


def main():
    print char_length(u'Hello你好，Fine')
    print char_length('Hello你好，Fine')

    # print type('Hello你好，Fine')
    # print type('Hello你好，Fine'.decode('utf-8').encode('gbk'))
    # print type(u'Hello你好，Fine')
    print char_length(u'H你好，'.encode('gbk'), 'gbk')
    # returncode, out_lines = execute_command('dir')
    # for line in out_lines:
    #     print line
    # returncode, out_lines, err_lines = execute_command('ping 128.196.100.146', timeout=20, logFile='ping')    # pass
    # returncode, out_lines, err_lines = execute_command('ls -l /home/ap', logFile='ping')    # pass
    # print dateValid('20170101')
    # print dateValid('201701010')
    # print dateValid('20172101')
    # print dateValid('20171132')
    # print dateValid('20171131')
    # print dateValid('20171130')
    # print findFile('/home/pi/ts150/non_self', '7_predict.sh')
    # print findFile('/home/pi/ts150/non_self', 'a.txt')
    # executeShell('echo $pid; sleep 10')
    # print exist_pid('17930')
    # print exist_pid(17930)
    # print exist_pid(178923)

    # now = today('%Y-%m-%d %H:%M:%S')
    # print now
    # print time.localtime()
    # print timeCalc('2017-06-26 14:01:01')
    # print timeCalc('2017-06-27 17:01:01')
    # print timeCalc('2017-06-27 17:01:01', 'minute')
    # print timeCalc('2017-06-27 17:01:01', 'hour')
    # print timeCalc('2017-05-26 14:01:01')
    # print timeCalc('2017-05-26', 'week')
    # print timeCalc('2017-05-26', 'month')
    # print timeCalc('2017-05-26 14:01:01', 'day')
    # print get_ip()


if __name__ == '__main__':
    # main()
    # print '们吴'
    # raise CommonError(msg='命令出错:%s\n%s' % ('hello', '\n'.join(['们', 'hello'])))
    for data_date in getDateList('20170204', '20170215', 7):
        print data_date
