#!/usr/bin/env python
#coding:utf8

import sys,os
import dbAdapter
sys.path.append("../python_common/")
import log


# 作业调度数据表操作
class DbScheduler:
    def __init__(self, remote_mode=True):
        self.db = dbAdapter.DbAdapter(remote_mode)


    # 实体表查询
    def query_entity(self, id=0, where=''):
        log.debug('query_entity:[%d][%s]' % (id, where))

        if id > 0:
            where = 'where id=%d' % id
        sql = "select * from entity %s" % where
        row_array = self.db.select(sql)

        return row_array


    # 通过ID实体表更新
    def update_entity_from_id(self, id, status=None, file_num=None, file_size=None, partition_num=None, record_num=None):
        log.debug('update_entity_from_id:[%s][%s]' % (id, status))

        update_field_array = ["ts=datetime('now', 'localtime')", ]
        if status:
            update_field_array.append("status='%s'" % status)
        if file_num:
            update_field_array.append("file_num=%d" % file_num)
        if file_size:
            update_field_array.append("file_size=%d" % file_size)
        if partition_num:
            update_field_array.append("partition_num=%d" % partition_num)
        if record_num:
            update_field_array.append("record_num=%d" % record_num)

        sql = "update entity set %s where id=%d" % (', '.join(update_field_array), id)

        return self.db.update(sql)


    # 实体表保存
    def save_entity(self, flag, entity, data_date, status=None, file_num=None, file_size=None, partition_num=None, record_num=None):
        log.debug('save_entity:[%s][%s][%s][%s]' % (flag, entity, data_date, status))
        where = "where flag='%s' and entity='%s' and data_date='%s'" % \
                (flag, entity, data_date)

        row_array = self.query_entity(where=where)

        # 如果记录不存在，则新增
        if len(row_array) == 0:
            sql = "insert into entity(flag, entity, data_date) values('%s', '%s', '%s')" % \
                  (flag, entity, data_date)

            return self.db.insert(sql)

        updated_num = 0
        for row in row_array:
            updated_num += self.update_entity_from_id(row['id'], status, file_num, file_size, partition_num, record_num)

        return updated_num


    # 作业配置表查询
    def query_work_config(self, id=0, where=''):
        if id > 0:
            where = 'where id=%d' % id
        sql = "select * from work_config %s" % where

        row_array = self.db.select(sql)
        for row in row_array:
            row['base_script'] = os.path.basename(row['script'])

        return row_array


    # 作业配置表插入
    def insert_work_config(self, script, start_date, end_date, hostname,
                           username, step=1, force=False, over_notice=False,
                           error_notice=False):
        sql = "insert into work_config("
        sql += " script, start_date, end_date, hostname, username, step, force, over_notice, error_notice)"
        sql += " values('%s', '%s', '%s', '%s', '%s', %d, %d, %d, %d)" % \
               (script, start_date, end_date, hostname, username, step,
                force, over_notice, error_notice)

        return self.db.insert(sql)


    # 作业配置表更新
    def update_work_config(self, id, end_date=None,
                           force=None, over_notice=None, error_notice=None,
                           over_date=None, status=None, pid=None,
                           process_date=None, next_action=None):
        where = 'where id=%d' % id
        update_field_array = ["ts=datetime('now', 'localtime')", ]
        if over_date:
            update_field_array.append("over_date='%s'" % over_date)
        if end_date:
            update_field_array.append("end_date='%s'" % end_date)
        if status:
            update_field_array.append("status='%s'" % status)
        if pid != None:
            update_field_array.append("pid=%d" % pid)
        if process_date:
            update_field_array.append("process_date='%s'" % process_date)
        if next_action:
            update_field_array.append("next_action='%s'" % next_action)
        if force != None:
            update_field_array.append("force=%d" % force)
        if over_notice != None:
            update_field_array.append("over_notice=%d" % over_notice)
        if error_notice != None:
            update_field_array.append("error_notice=%d" % error_notice)

        sql = "update work_config set %s %s" % (', '.join(update_field_array), where)

        return self.db.update(sql)


    # 作业配置表删除
    def delete_work_config(self, id=0, where=''):
        if id > 0:
            where = 'where id=%d' % id
        sql = "delete from work_config %s" % where

        return self.db.delete(sql)


    # 定时作业配置表查询
    def query_crontab_config(self, id=0, where=''):
        if id > 0:
            where = 'where id=%d' % id
        sql = "select * from crontab_config %s" % where

        row_array = self.db.select(sql)

        for row in row_array:
            row['base_script'] = os.path.basename(row['script'])

        return row_array


    # 定时作业配置表保存
    def save_crontab_config(self, id=0, crontab=None, script=None, status=None,
                            pid=None, hostname=None, username=None, next_action=None,
                            error_notice=None):
        where = "where crontab='%s' and script='%s'" % (crontab, script)
        row_array = self.query_crontab_config(id, where)

        if len(row_array) == 0:
            sql = "insert into crontab_config(crontab, script, hostname, username, error_notice) values('%s', '%s', '%s', '%s', %d)" % \
                  (crontab, script, hostname, username, error_notice)
            # print sql
            return self.db.insert(sql)
        else:
            if id > 0:
                where = 'where id=%d' % id
            update_field_array = ["ts=datetime('now', 'localtime')", ]
            if crontab:
                update_field_array.append("crontab='%s'" % crontab)
            if script:
                update_field_array.append("script='%s'" % script)
            if status:
                update_field_array.append("status='%s'" % status)
            if pid != None:
                update_field_array.append("pid=%d" % pid)
            if hostname:
                update_field_array.append("hostname='%s'" % hostname)
            if username:
                update_field_array.append("username='%s'" % username)
            if next_action:
                update_field_array.append("next_action='%s'" % next_action)
            if error_notice:
                update_field_array.append("error_notice='%s'" % error_notice)

            sql = "update crontab_config set %s %s" % (', '.join(update_field_array), where)

            return self.db.update(sql)


    # 作业配置表删除
    def delete_crontab_config(self, id=0, where=''):
        if id > 0:
            where = 'where id=%d' % id
        sql = "delete from crontab %s" % where

        return self.db.delete(sql)


def main():
    db = DbScheduler(remote_mode=False)
    print db.save_entity('LOCAL', '/root', '20170101')
    print db.query_entity(where="where flag='LOCAL'")
    print db.save_entity('LOCAL', '/root', '20170101', status='SUCC', file_num=10, file_size=5466515, partition_num=1, record_num=100)
    print db.query_entity(where="where flag='LOCAL'")
    print db.insert_work_config('script', '20170101', '20170101', 'hostname',
                                'username', step=1, force=False, over_notice=False,
                                error_notice=False)
    print db.query_work_config(where="where script='script'")
    print db.update_work_config(id=1, end_date=None,
                                force=True, over_notice=None, error_notice=None,
                                over_date='20170102', status='over', pid=100,
                                process_date=None, next_action=None)
    print db.query_work_config(where="where script='script'")
    print db.delete_work_config(where="where script='script'")


if __name__ == '__main__':
    main()
    # print sys.argv
    # for var in getVar(sys.argv[1]):
    #     print var
