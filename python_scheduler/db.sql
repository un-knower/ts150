-- sqlite3 violate.db

drop table if exists log;

create table log(
   id INTEGER PRIMARY KEY AUTOINCREMENT,
   ts datetime default (datetime('now', 'localtime')),
   wc_id INTEGER,
   msg nvarchar(1024),
   ret int,
   level varchar(10) default 'info',
   name varchar(255),
   args varchar(255),
   duration time,
   user varchar(32)
);

-- insert into log(msg, ret) values('Hello World', 1);

-- UPDATE log t1, (select ts, id from log where id = (select max(id) from log where pid=15472)) t2
--  SET t1.duration=t2.duration WHERE t1.id=t2.id;

-- select ts from log where id = (select max(id) from log where pid=15472);


-- 任务完成情况表
drop table if exists task;

create table task(
   id INTEGER PRIMARY KEY AUTOINCREMENT,
   wc_id INTEGER,
   ts datetime default (datetime('now', 'localtime')),
   fun varchar(255) not null,
   data_date varchar(8) not null,
   ret int,
   path varchar(255),
   name varchar(255),
   args varchar(255),
   user varchar(32)
);

-- 实体完成情况表
drop table if exists entity;

create table entity(
   id INTEGER PRIMARY KEY AUTOINCREMENT,
   ts datetime default (datetime('now', 'localtime')),
   flag varchar(8) not null,
   entity varchar(255) not null,
   data_date varchar(8) not null,
   status varchar(16) default(''),
   file_num INTEGER default 0,
   file_size INTEGER default 0,
   partition_num INTEGER default 0,
   record_num INTEGER default 0
);


-- 连续作业配置表
drop table if exists work_config;

create table work_config(
   id INTEGER PRIMARY KEY AUTOINCREMENT,
   ts datetime default (datetime('now', 'localtime')),
   script varchar(255) not null,
   start_date varchar(8) not null,
   end_date varchar(8) not null,
   hostname varchar(64) default(''),
   username varchar(64) default(''),
   step INTEGER default 1,             -- 下一日期步进
   force INTEGER default 0,            -- 强制
   over_notice INTEGER default 0,      -- 完成通知
   error_notice INTEGER default 0,     -- 出错通知
   process_date varchar(8) default(''),
   over_date varchar(8) default(''),
   status varchar(16) default(''),     -- 完成情况
   next_action varchar(16) default(''),
   pid INTEGER default 0,              -- 处理进程ID
   pre_work_id INTEGER default 0       -- 跟随上一作业
);


-- 定时作业配置表
drop table if exists crontab_config;

create table crontab_config(
   id INTEGER PRIMARY KEY AUTOINCREMENT,
   ts datetime default (datetime('now', 'localtime')),
   crontab varchar(255) not null,
   script varchar(255) not null,
   status varchar(16) default(''),     -- 完成情况
   next_action varchar(16) default(''),
   pid INTEGER default 0,         -- 处理进程ID
   hostname varchar(64) default(''),
   username varchar(64) default(''),
   error_notice INTEGER default 0     -- 出错通知
);

-- 守护进程情况
drop table if exists daemon_running;

create table daemon_running(
   id INTEGER PRIMARY KEY AUTOINCREMENT,
   ts datetime default (datetime('now', 'localtime')),
   hostname varchar(64),
   username varchar(64),
   pid INTEGER default 0,         -- 处理进程ID
   daemon varchar(64) not null,
   process_name varchar(255),
   ppid INTEGER default 0,        -- Daemon进程ID
   start_time datetime,  -- 启动时间
   status varchar(16) default(''), -- 状态
   renew_time datetime             -- 刷新时间
);
