use sor;
-- 建行机构关系 T0651_CCBINS_REL_H 拉链处理

-- 复制贴源数据
INSERT OVERWRITE TABLE CT_T0651_CCBINS_REL_H_MID PARTITION(DATA_TYPE='${log_date}_INC')
SELECT 
       -- 主建行机构编号
       a.PRIM_CCBINS_ID,
       -- 建行机构内部层级结构类型代码
       a.CCBINSINRHIERSTCTP_CD,
       -- 建行机构层级结构名称
       a.CCBINS_HIERSTC_NM,
       -- 从建行机构编号
       a.SCDY_CCBINS_ID,
       -- 机构管理等级描述
       a.INSTMGTGRD_DSC,
       -- 关系场景描述
       a.REL_SCN_DSC,
       -- #最后更新日期时间
       a.LAST_UDT_DT_TM,
       -- P9开始日期
       a.P9_START_DATE,
       -- P9结束日期
       a.P9_END_DATE
  FROM INN_T0651_CCBINS_REL_H a
 WHERE LOAD_DATE='${log_date}';

-- 去重
INSERT OVERWRITE TABLE CT_T0651_CCBINS_REL_H_MID PARTITION(DATA_TYPE='${log_date}_ALL')
SELECT 
       -- 主建行机构编号
       a.PRIM_CCBINS_ID,
       -- 建行机构内部层级结构类型代码
       a.CCBINSINRHIERSTCTP_CD,
       -- 建行机构层级结构名称
       a.CCBINS_HIERSTC_NM,
       -- 从建行机构编号
       a.SCDY_CCBINS_ID,
       -- 机构管理等级描述
       a.INSTMGTGRD_DSC,
       -- 关系场景描述
       a.REL_SCN_DSC,
       -- #最后更新日期时间
       a.LAST_UDT_DT_TM,
       -- P9开始日期
       a.P9_START_DATE,
       -- P9结束日期
       a.P9_END_DATE
  FROM (SELECT PRIM_CCBINS_ID, CCBINSINRHIERSTCTP_CD, CCBINS_HIERSTC_NM, SCDY_CCBINS_ID,
               INSTMGTGRD_DSC, REL_SCN_DSC, LAST_UDT_DT_TM, 
               P9_START_DATE, P9_END_DATE,
               row_number() over (
                    partition by PRIM_CCBINS_ID, CCBINSINRHIERSTCTP_CD, CCBINS_HIERSTC_NM, SCDY_CCBINS_ID,
               INSTMGTGRD_DSC, REL_SCN_DSC, LAST_UDT_DT_TM
                    order by P9_START_DATE
                   ) rownum
         FROM CT_T0651_CCBINS_REL_H_MID 
        WHERE DATA_TYPE in ('${log_date}_INC', '${log_date_less_1}_ALL') 
        ) a
 WHERE a.rownum = 1;

-- Hive动态分区参数设置
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=strick;

-- 重建拉链
INSERT OVERWRITE TABLE CT_T0651_CCBINS_REL_H PARTITION(P9_END_DATE)
SELECT PRIM_CCBINS_ID, CCBINSINRHIERSTCTP_CD, CCBINS_HIERSTC_NM, SCDY_CCBINS_ID,
               INSTMGTGRD_DSC, REL_SCN_DSC, LAST_UDT_DT_TM, 
       P9_START_DATE, 
       lead(P9_START_DATE, 1, '29991231') over (partition by PRIM_CCBINS_ID, SCDY_CCBINS_ID order by P9_START_DATE) as P9_END_DATE
  FROM CT_T0651_CCBINS_REL_H_MID
 WHERE DATA_TYPE='${log_date}_ALL';
