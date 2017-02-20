use sor;

# Hive贴源数据处理
# 员工: T0861_EMPE_H

-- 指定新数据日期分区位置
ALTER TABLE EXT_T0861_EMPE_H DROP IF EXISTS PARTITION(LOAD_DATE='${log_date}');
ALTER TABLE EXT_T0861_EMPE_H ADD PARTITION (LOAD_DATE='${log_date}') LOCATION 'hdfs://hacluster/bigdata/input/case_trace/T0861_EMPE_H/${log_date}/';

-- 备份贴源数据到ORC内部表
INSERT OVERWRITE TABLE INN_T0861_EMPE_H PARTITION(LOAD_DATE='${log_date}')
SELECT 
   -- 建行员工编号
   CCB_EMPID,
   -- 性别代码
   GND_CD,
   -- 用户姓名
   USR_NM,
   -- 人力资源员工编号
   HMNRSC_EMPID,
   -- 员工识别登录名
   EMPE_ID_LAND_NM,
   -- 证件类型代码
   CRDT_TPCD,
   -- 证件号码
   CRDT_NO,
   -- 用户类型代码
   USR_TPCD,
   -- 用户状态代码
   USR_STCD,
   -- 电话联系方式办公号码
   TELCTCMOD_WRK_NO,
   -- 电话联系方式手机号码
   TELCTCMOD_MBLPH_NO,
   -- 电话联系方式传真号码
   TELCTCMOD_FAX_NO,
   -- 电话联系方式备用号码
   TELCTCMOD_RSRV_NO,
   -- 电子邮件地址内容
   EMAIL_ADR_CNTNT,
   -- 个人名称姓氏
   IDV_NM_SURNM,
   -- 个人名称名
   IDV_NM_NM,
   -- 邮政编码
   ZIPECD,
   -- 详细地址
   DTL_ADR,
   -- 所属机构编号
   BLNG_INSID,
   -- 工作单位名称
   WRK_UNIT_NM,
   -- 外部用户职务
   EXT_USR_POST,
   -- 学历代码
   EDDGR_CD,
   -- 学位代码
   DGR_CD,
   -- 毕业学校代码
   GRDT_SCH_CD,
   -- 学校性质代码
   SCH_CHAR_CD,
   -- 所学专业代码
   MJR_CD,
   -- 学习形式代码
   STDY_FORM_CD,
   -- 用户姓名拼音全称
   USR_NM_CPA_FULLNM,
   -- 员工岗位锁定状态代码
   EMPE_PST_LOCK_STCD,
   -- 生效日期时间
   EFDT_TM,
   -- 创建系统管理操作员编号
   CRT_STM_MGT_OPR_ID,
   -- #最后更新日期时间
   LAST_UDT_DT_TM,
   -- 其他专业名称
   OTHR_PROF_NM,
   -- P9开始日期
   regexp_replace(P9_START_DATE, '-', ''),
   -- P9结束日期
   regexp_replace(P9_END_DATE, '-', ''),
   -- 员工照片文件索引号
   EMPE_PIC_FILE_INDX_NO,
   -- 出生日期
   regexp_replace(BRTH_DT, '-', ''),
   -- 紧急联系方式内容
   regexp_replace(EMGR_CTC_MOD_CNTNT, '-', ''),
   -- 毕业日期
   regexp_replace(GRDT_DT, '-', '')
  FROM EXT_T0861_EMPE_H
 WHERE LOAD_DATE='${log_date}';
