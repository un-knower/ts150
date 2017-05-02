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
   flag char(1) not null,
   entity varchar(255) not null,
   data_date varchar(8) not null,
   status varchar(16) default(''),
   pid INTEGER default 0         -- 处理进程ID
);

-- 作业配置表
drop table if exists work_config;

create table work_config(
   id INTEGER PRIMARY KEY AUTOINCREMENT,
   ts datetime default (datetime('now', 'localtime')),
   script varchar(255) not null,
   options varchar(255) not null,
   start_date varchar(8) not null,
   end_date varchar(8) not null,
   priority INTEGER default(1),
   over_date varchar(8) default(''),
   status varchar(16) default(''),     -- 完成情况
   pid INTEGER default 0         -- 处理进程ID
);
