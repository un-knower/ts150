use sor;
-- 员工 T0861_EMPE_H 拉链处理

-- 复制贴源数据
INSERT OVERWRITE TABLE INN_T0861_EMPE_H_MID PARTITION(DATA_TYPE='SRC')
SELECT 
       -- 建行员工编号
       a.CCB_EMPID,
       -- 性别代码
       a.GND_CD,
       -- 用户姓名
       a.USR_NM,
       -- 人力资源员工编号
       a.HMNRSC_EMPID,
       -- 员工识别登录名
       a.EMPE_ID_LAND_NM,
       -- 证件类型代码
       a.CRDT_TPCD,
       -- 证件号码
       a.CRDT_NO,
       -- 用户状态代码
       a.USR_STCD,
       -- 电话联系方式办公号码
       a.TELCTCMOD_WRK_NO,
       -- 电话联系方式手机号码
       a.TELCTCMOD_MBLPH_NO,
       -- 个人名称姓氏
       a.IDV_NM_SURNM,
       -- 个人名称名
       a.IDV_NM_NM,
       -- 详细地址
       a.DTL_ADR,
       -- 所属机构编号
       a.BLNG_INSID,
       -- 生效日期时间
       a.EFDT_TM,
       -- P9开始日期
       a.P9_START_DATE,
       -- P9结束日期
       a.P9_END_DATE
  FROM EXT_T0861_EMPE_H a
 WHERE LOAD_DATE='${log_date}';

-- 去重
INSERT OVERWRITE TABLE CT_T0861_EMPE_H_MID PARTITION(DATA_TYPE='CUR_NO_DUP')
SELECT 
       -- 建行员工编号
       a.CCB_EMPID,
       -- 性别代码
       a.GND_CD,
       -- 用户姓名
       a.USR_NM,
       -- 人力资源员工编号
       a.HMNRSC_EMPID,
       -- 员工识别登录名
       a.EMPE_ID_LAND_NM,
       -- 证件类型代码
       a.CRDT_TPCD,
       -- 证件号码
       a.CRDT_NO,
       -- 用户状态代码
       a.USR_STCD,
       -- 电话联系方式办公号码
       a.TELCTCMOD_WRK_NO,
       -- 电话联系方式手机号码
       a.TELCTCMOD_MBLPH_NO,
       -- 个人名称姓氏
       a.IDV_NM_SURNM,
       -- 个人名称名
       a.IDV_NM_NM,
       -- 详细地址
       a.DTL_ADR,
       -- 所属机构编号
       a.BLNG_INSID,
       -- 生效日期时间
       a.EFDT_TM,
       -- P9开始日期
       a.P9_START_DATE,
       -- P9结束日期
       a.P9_END_DATE
  FROM (SELECT , P9_START_DATE, P9_END_DATE,
               row_number() over (
                    partition by 
                    order by P9_START_DATE
                   ) rownum
         FROM CT_T0861_EMPE_H_MID 
        WHERE DATA_TYPE in ('SRC', 'PRE_NO_DUP') 
        ) a
 WHERE a.rownum = 1;

-- Hive动态分区参数设置
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=strick;

-- 重建拉链
INSERT OVERWRITE TABLE CT_T0861_EMPE_H PARTITION(P9_END_DATE)
SELECT , P9_START_DATE, 
       lead(P9_START_DATE, 1, '29991231') over (partition by CCB_EMPID order by P9_START_DATE) as P9_END_DATE
  FROM CT_T0861_EMPE_H_MID
 WHERE DATA_TYPE='CUR_NO_DUP';

------------ 以下操作可以导致数据丢失 ---------------
-- 备份当前非重复数据 到 PRE_NO_DUP 分区
ALTER TABLE CT_T0861_EMPE_H_MID DROP IF EXISTS PARTITION(DATA_TYPE='PRE_NO_DUP');

ALTER TABLE CT_T0861_EMPE_H_MID PARTITION(DATA_TYPE='CUR_NO_DUP') 
   RENAME TO PARTITION(DATA_TYPE='PRE_NO_DUP');

ALTER TABLE CT_T0861_EMPE_H_MID ADD IF NOT EXISTS PARTITION(DATA_TYPE='CUR_NO_DUP');
