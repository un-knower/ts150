use sor;

-- Hive贴源数据处理
-- 建行机构关系: T0651_CCBINS_REL_H

-- 指定新数据日期分区位置
ALTER TABLE EXT_T0651_CCBINS_REL_H DROP IF EXISTS PARTITION(LOAD_DATE='${log_date}');
ALTER TABLE EXT_T0651_CCBINS_REL_H ADD PARTITION (LOAD_DATE='${log_date}') LOCATION 'hdfs://hacluster/bigdata/input/TS150/case_trace/T0651_CCBINS_REL_H/${log_date}/';

-- 备份贴源数据到ORC内部表
INSERT OVERWRITE TABLE INN_T0651_CCBINS_REL_H PARTITION(LOAD_DATE='${log_date}')
SELECT 
   -- 主建行机构编号
   PRIM_CCBINS_ID,
   -- 建行机构内部层级结构类型代码
   CCBINSINRHIERSTCTP_CD,
   -- 建行机构层级结构描述
   CCBINS_HIERSTC_DSC,
   -- 建行机构层级结构名称
   CCBINS_HIERSTC_NM,
   -- 建行机构层级结构状态类型代码
   CCBINSHIERSTCSTTP_CD,
   -- 从建行机构编号
   SCDY_CCBINS_ID,
   -- 建行机构和建行机构关系类型代码
   CCBIANDCCBIRELTP_CD,
   -- 关系方向描述
   REL_DRC_DSC,
   -- 建行机构和建行机构关系生命周期状态代码
   CCBIACCBI_REL_LCS_CD,
   -- 建行机构和建行机构关系生命周期状态日期
   CCBIACCBI_REL_LCS_DT,
   -- 建行机构和建行机构关系生命周期状态原因描述
   CCBIANDCCBIRLCSR_DSC,
   -- 建行机构和建行机构关系生命周期变更序号
   CCBIACCBI_REL_LCMD_SN,
   -- 机构管理等级类型代码
   INSTMGTGRD_TPCD,
   -- 机构管理等级类型取值代码
   INSTMGTGRD_TP_VAL_CD,
   -- 机构管理等级描述
   INSTMGTGRD_DSC,
   -- 关系场景描述
   REL_SCN_DSC,
   -- #最后更新日期时间
   LAST_UDT_DT_TM,
   -- P9开始日期
   regexp_replace(P9_START_DATE, '-', ''),
   -- P9结束日期
   regexp_replace(P9_END_DATE, '-', '')
  FROM EXT_T0651_CCBINS_REL_H
 WHERE LOAD_DATE='${log_date}';
