#!/usr/bin/python
#coding:utf8

import os
from var import *
from dbClient import *
import check
sys.path.append("../python_common/")
from common_fun import *
import log


# 调度类
class Scheduler:
    """调度类"""
    def __init__(self, options=None, scriptName=None, db=None):
        self.options = options
        self.scriptName = scriptName

        self.db = db if db else DbHelper()

        self.depend_key_list = ('IN_CUR_HIVE', 'IN_PRE_HIVE', 'IN_CUR_HDFS', 'IN_CUR_GP', 'IN_PRE_GP', 'IN_CUR_LOCAL',
                                'IN_ALL_HIVE',
                                'OUT_CUR_HIVE', 'OUT_CUR_GP', 'OUT_CUR_LOCAL')


        if options and options.username:
            tmp_array = options.username.split('.')
            if len(tmp_array) == 2:
                self.hostname = tmp_array[0]
                self.username = tmp_array[1]
            else:
                tmp_array = options.username.split('@')
                assert len(tmp_array) == 2
                self.hostname = tmp_array[1]
                self.username = tmp_array[0]
        else:
            self.hostname = hostname
            self.username = username


    # 插入脚本依赖项到实体表
    def insert_depend_entity(self, scriptName, start_date, end_date):
        script_depend_array = self.get_script_depend(scriptName)

        for k,entity in script_depend_array:
            depend_condition_array = k.split('_')

            # 检查日期判断
            depend_date = depend_condition_array[1]
            if 'CUR' == depend_date:
                start_date = start_date
            if 'PRE' == depend_date:
                start_date = dateCalc(start_date, -1)
            if 'ALL' == depend_date:
                start_date = ''
                end_date = ''

            depend_type = depend_condition_array[2]

            # 按日期一天天增加实体
            if start_date and end_date:
                for data_date in getDateList(start_date, end_date):
                    self.db.insert_entity(depend_type, entity, data_date)
            else:
                # 实体无日期分区，只需表存在即可
                self.db.insert_entity(depend_type, entity, '')


    # 获取脚本依赖项
    def get_script_depend(self, scriptName=None):
        if not scriptName:
            scriptName = self.scriptName
        kv_array = []
        script_type = get_script_type(scriptName)
        if script_type == '.sh':
            kv_array = getRemarkKV(scriptName, None)
        elif script_type in ('.py', '.pl'):
            kv_array = getRemarkKV(scriptName, '#')
        elif script_type in ('.sql', '.gpsql'):
            kv_array = getRemarkKV(scriptName, '--')
        else:
            pass

        # 可定义多行依赖项，以数组表示
        depend_array = []

        for (key, entity) in kv_array:
            if key in self.depend_key_list:
                if '' == entity:
                    continue

                depend_condition_array = key.split('_')

                # 检查输入项格式
                assert len(depend_condition_array) == 3

                depend_type = depend_condition_array[2]

                if 'HIVE' == depend_type and len(entity.split('.')) == 1:
                    entity = '%s.%s' % (default_hive_db, entity)

                if 'GP' == depend_type and len(entity.split('.')) == 1:
                    entity = '%s.%s' % (default_gp_schema, entity)

                depend_array.append((key, entity))

        return depend_array


    # 判断脚本依赖项是否准备好
    def check_script_depend(self, scriptName, data_date, io='in', viaTable=False):
        # 判断脚本输入依赖
        depend_array = self.get_script_depend(scriptName)
        for key, entity in depend_array:
            # 不符合条件的，不检查
            depend_condition_array = key.split('_')

            # 检查输入项格式
            assert len(depend_condition_array) == 3

            depend_io = depend_condition_array[0]
            depend_date = depend_condition_array[1]
            depend_type = depend_condition_array[2]

            if 'IN' != depend_io and io == 'in': continue
            if 'OUT' != depend_io and io == 'out': continue

            assert depend_date in ('CUR', 'PRE', 'ALL')

            # 检查日期判断
            if 'CUR' == depend_date:
                check_date = data_date

            if 'PRE' == depend_date:
                check_date = dateCalc(data_date, -1)

            if 'ALL' == depend_date:
                check_date = ''

            # print viaTable
            if viaTable:
                # 通过entity表检查
                valid_success = self.check_entity_via_table(depend_type, entity, check_date, logFile=scriptName)
                if not valid_success:
                    valid_success = self.check_entity(depend_type, entity, check_date, logFile=scriptName)
            else:
                # 通过实体检查
                valid_success = self.check_entity(depend_type, entity, check_date, logFile=scriptName)

            # 检查实体不存在，返回False
            if not valid_success:
                log.info('脚本:[%s]对应的实体[%s]:[%s][%s]在日期:[%s]的未完成' % (
                    scriptName, io, key, entity, check_date), scriptName)
                return valid_success

        log.info('脚本:[%s]对应的实体[%s]在日期:[%s]的已完成, 通过' % (scriptName, io, data_date), scriptName)

        return True


    # 判断实体是否准备好
    def check_entity(self, depend_type, entity, check_date, logFile=None):
        # 输入项名称或实体类型标志必输一项
        assert depend_type in ('HDFS', 'HIVE', 'GP', 'LOCAL')

        # 不检查应用的第一天的 PRE 数据
        if check_date < app_start_date:
            self.db.update_entity(depend_type, entity, check_date, 'exist', os.getpid())
            return True

        self.db.update_entity(depend_type, entity, check_date, 'processing', os.getpid())

        valid_success = False
        if 'HDFS' == depend_type:
            valid_success = check.valid_hdfs_file(entity, check_date, logFile=logFile)

        elif 'HIVE' == depend_type:
            valid_success = check.valid_hive_table(entity, check_date, logFile=logFile)

        elif 'GP' == depend_type:
            valid_success = check.valid_gp_table(entity, check_date, logFile=logFile)

        elif 'LOCAL' == depend_type:
            valid_success = check.valid_local_file(entity, check_date, logFile=logFile)

        self.db.update_entity(depend_type, entity, check_date, 'exist' if valid_success else 'not exist', os.getpid())

        # 检查实体不存在，返回False
        if not valid_success:
            return valid_success

        return True


    # 通过表判断实体是否准备好
    def check_entity_via_table(self, flag, entity, data_date, logFile=None):
        where = "where flag='%s' and entity='%s' and data_date='%s'" % \
                (flag, entity, data_date)

        row_map = self.db.query_entity(where=where)
        if len(row_map) < 1:
            log.error('entity表找不到应对实体记录：[%s][%s][%s]' % (flag, entity, data_date), logFile)
            return False

        row = row_map.values()[0]

        return True if 'exist' == row['status'] else False


    def run_shell(self, row, data_date):
        options = row['options']
        scriptName = options['script']

        # 运行脚本
        cmd = '%s -d %s' % (row['script'], data_date)
        log.info(cmd, scriptName)

        (returncode, out_lines, err_lines) = execute_command(cmd, logFile=scriptName)

        log.info(returncode, scriptName)
        # for line in out_lines:
        #     log.info(line, scriptName)

        # 判断脚本输出
        if returncode == 0:
            return 'over'

        return 'fail'


    def run_hive_sql(self, row, data_date):
        script = row['script']

        cmd = 'beeline -f %s' % script

        patt = re.compile(r'less_(\d+)_date')

        # 对SQL中的变量提供支持
        for var in getVar(script):
            if var == 'db':
                value = default_hive_db
            elif var in ('log_date', 'p9_data_date', 'data_date'):
                value = data_date
            elif var in ('IN_ALL_HIVE', 'OUT_CUR_HIVE'):
                depend_array = self.get_script_depend(script)
                for k,v in depend_array:
                    if k == var:
                        value = v
            else:
                m = patt.match(var)
                if m:
                    days = m.group(1)
                    value = dateCalc(data_date, - int(days))
                else:
                    raise CommonError(msg='Hive变量未定义:[%s]' % var)

            cmd += ' --hivevar %s="%s"' % (var, value)

        log.info(cmd, script)

        # 运行脚本
        # (returncode, out_lines) = executeShell_ex(cmd)
        (returncode, out_lines, err_lines) = execute_command(cmd, logFile=script)

        log.info(returncode, script)

        # for line in out_lines:
        #     log.info(line, script)

        # 判断脚本输出
        if returncode == 0:
            return 'over'

        return 'fail'


    def run_gp_sql(self, row, data_date):
        pass

    def run_python(self, row, data_date):
        pass

    # 加入新的作业配置与实体依赖
    def add_work_config(self):
        print self.db.insert_work_config(self.scriptName, self.options, self.options.start_date,
                                         self.options.end_date, self.options.priority, self.hostname, self.username)
        self.insert_depend_entity(self.scriptName, self.options.start_date, self.options.end_date)


    # 加入定时作业配置
    def add_crontab_config(self):
        sched.db.save_crontab_config(crontab=self.options.crontab, script=self.scriptName,
                                     options=self.options, hostname=self.hostname, username=self.username)


def main():
    pass


if __name__ == '__main__':
    main()
