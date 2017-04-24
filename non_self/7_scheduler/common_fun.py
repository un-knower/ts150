#!/usr/bin/python
#coding:gbk
'''
    ���ú�����           2016.2.27
'''

import sys, os, time, datetime
import platform
import socket
import subprocess
import shlex
import commands

# ȡ������
hostname = socket.gethostname()
save_path = './'

def today(fmt='%Y%m%d'):
    '''ȡ��ǰ�����ַ���YYYYMMDD'''
    t = time.strftime(fmt, time.localtime())
    return t


def dateCalc(strDate, iDay, fmt='%Y%m%d'):
    ''' �������� '''
    chkDate = time.strptime(strDate, fmt)
    dateTmp = datetime.datetime(chkDate[0], chkDate[1], chkDate[2])
    resultDate = dateTmp + datetime.timedelta(days=iDay)
    return datetime.date.strftime(resultDate, fmt)


def dateValid(strDate, fmt='%Y%m%d'):
    ''' ���ڸ�ʽ���� '''
    try:
        chkDate = time.strptime(strDate, fmt)
        # print chkDate
        return True
    except Exception as e:
        return False


def execute_command(cmdstring, cwd=None, timeout=None, shell=True):
    """ִ��һ��SHELL����
            ��װ��subprocess��Popen����, ֧�ֳ�ʱ�жϣ�֧�ֶ�ȡstdout��stderr
           ����:
        cwd: ��������ʱ����·����������趨���ӽ��̻�ֱ���ȸ��ĵ�ǰ·����cwd
        timeout: ��ʱʱ�䣬�룬֧��С��������0.1��
        shell: �Ƿ�ͨ��shell����
    Returns: return_code
    Raises:  Exception: ִ�г�ʱ
    """
    if shell:
        cmdstring_list = cmdstring
    else:
        cmdstring_list = shlex.split(cmdstring)
    if timeout:
        end_time = datetime.datetime.now() + datetime.timedelta(seconds=timeout)

    log.info('��ʼ������:[%s]��ִ��Shell����:[%s]' % (hostname, cmdstring))

    # ����һ��Shell����ִ�з���
    # returncode, out = commands.getstatusoutput(cmdstring)
    returncode, out = commands.getstatusoutput('dir')
    print out
    out_lines = out.split('\n')
    err_lines = []
    # print returncode
    # print out_lines

    #û��ָ����׼����ʹ�������Ĺܵ�����˻��ӡ����Ļ�ϣ�
    # sub = subprocess.Popen(cmdstring_list, cwd=cwd, 
    #     stdin=subprocess.PIPE,
    #     stdout=subprocess.PIPE,
    #     stderr=subprocess.PIPE,
    #     shell=shell,bufsize=1024000)
    
    # #subprocess.poll()����������ӽ����Ƿ�����ˣ���������ˣ��趨�������룬����subprocess.returncode������ 
    # while sub.poll() is None:
    #     time.sleep(0.1)
    #     if timeout:
    #         if end_time <= datetime.datetime.now():
    #             raise Exception("Timeout��%s"%cmdstring)
     
    # out_lines = sub.stdout.readlines()
    # err_lines = sub.stderr.readlines()
    # returncode = sub.returncode

    # log.info('���������:[%s]��ִ��Shell����:[%s], ����ֵ:[%d]' % (hostname, cmdstring, returncode))


    cmd = cmdstring.split(' ')[0]
    result_file_name = '%s/%s%s' % (save_path, cmd, today('_%H%M%S'))
    f = open(result_file_name, 'w')

    f.write('---------------shell---------------------\n')
    f.write(cmdstring + '\n')
    f.write('---------------------------------------\n')
    f.write('return code:[%d]\n' % returncode)
    f.write('--------------stdout--------------------\n')
    for line in out_lines:
        f.write(line + '\n')
    f.write('--------------stderr-------------------\n')
    for line in err_lines:
        f.write(line + '\n')
    f.close()

    return (returncode, out_lines)

class CommonError(Exception):
    """�����쳣��"""
    def __init__(self, code="", msg=""):
        self.code = code
        self.msg = msg
        
    def __repr__(self):
        return '�Զ��������Ϣ%s:%s' % self.code, self.msg


def isLinux():
    # print platform.system()
    if platform.system() == 'Linux':
        return True
    else:
        return False


def main():
    # returncode, out_lines = execute_command('dir')
    # for line in out_lines:
    #     print line
    # returncode, out_lines = execute_command('ping -n 1 192.168.19.130')
    # pass
    print dateValid('20170101')
    print dateValid('201701010')
    print dateValid('20172101')
    print dateValid('20171132')
    print dateValid('20171131')
    print dateValid('20171130')


if __name__ == '__main__':
    main()