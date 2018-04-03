-- sqlite3 monitor.db

-- 监控数据日期表
drop table if exists tbl_bussiness_date;

create table tbl_bussiness_date(
   bussiness_date_id INTEGER PRIMARY KEY AUTOINCREMENT,
   bussiness_type text,   -- 业务类型   nisalog/p2log/
   data_date text,   -- 数据日期
   monitor_num INTEGER default(0),      -- 监控次数
   is_report boolean default(0),        -- 是否已报告
   ts datetime default (datetime('now', 'localtime'))
);

-- 监控动作
drop table if exists tbl_monitor_action;

create table tbl_monitor_action(
   monitor_action_id INTEGER PRIMARY KEY AUTOINCREMENT,
   bussiness_date_id INTEGER,
   store_type text,  -- 存储类型 hdfs/nas/gp/hive/
   store_server text, -- 存储服务器 ts150_hdfs/ts150_nas/p9_hdfs/linux108164_local/ts150_hive

   cmd_text text,    -- 命令行
   exit_code INTEGER,-- 退出代码

   ts datetime default (datetime('now', 'localtime'))
);

-- 监控文件结果
drop table if exists tbl_monitor_file;

create table tbl_monitor_file(
   file_id  INTEGER PRIMARY KEY AUTOINCREMENT,
   monitor_action_id INTEGER,
   file_path text,
   file_status text,
   file_size INTEGER,
   file_mtime date,
   file_permission text,
   ts datetime default (datetime('now', 'localtime'))
);

drop table if exists tbl_monitor_table;

create table tbl_monitor_table(
   table_id  INTEGER PRIMARY KEY AUTOINCREMENT,
   monitor_action_id INTEGER,
   filepath text,
   filestatus text,
   size text,
   mtime date,

   ts datetime default (datetime('now', 'localtime'))
);
