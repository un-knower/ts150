use sor;

-- 中间表插入
-- 机构表: MID_CCBINS_CUR

-- 有关联的数据插入
INSERT OVERWRITE TABLE MID_CCBINS_CUR
SELECT
   -- 建行机构编号
   CCBINS_ID,
   -- 建行机构中文全称
   CCBINS_CHN_FULLNM,
   -- 建行机构中文简称
   CCBINS_CHN_SHRTNM,
   -- 建行机构英文全称
   CCBINS_ENG_FULLNM,
   -- 建行机构英文简称
   CCBINS_ENG_SHRTNM,
   -- 机构状态代码
   INST_STCD,
   -- 主建行机构编号
   PRIM_CCBINS_ID,
   -- 建行机构内部层级结构类型代码
   CCBINSINRHIERSTCTP_CD,
   -- 建行机构层级结构名称
   CCBINS_HIERSTC_NM,
   -- 从建行机构编号
   SCDY_CCBINS_ID,
   -- 机构管理等级描述
   INSTMGTGRD_DSC,
   -- 关系场景描述
   REL_SCN_DSC,
   -- #最后更新日期时间
   LAST_UDT_DT_TM FROM
(SELECT
        -- 建行机构编号
        CCBINS_ID,
        -- 建行机构中文全称
        CCBINS_CHN_FULLNM,
        -- 建行机构中文简称
        CCBINS_CHN_SHRTNM,
        -- 建行机构英文全称
        CCBINS_ENG_FULLNM,
        -- 建行机构英文简称
        CCBINS_ENG_SHRTNM,
        -- 机构状态代码
        INST_STCD
   FROM CT_T0651_CCBINS_INF_H
  WHERE P9_END_DATE = '29991231'
    AND (CCBINS_ID is not null AND CCBINS_ID <> '')) t
INNER JOIN
(SELECT
        -- 主建行机构编号
        PRIM_CCBINS_ID,
        -- 建行机构内部层级结构类型代码
        CCBINSINRHIERSTCTP_CD,
        -- 建行机构层级结构名称
        CCBINS_HIERSTC_NM,
        -- 从建行机构编号
        SCDY_CCBINS_ID,
        -- 机构管理等级描述
        INSTMGTGRD_DSC,
        -- 关系场景描述
        REL_SCN_DSC,
        -- #最后更新日期时间
        LAST_UDT_DT_TM
   FROM CT_T0651_CCBINS_REL_H
  WHERE P9_END_DATE = '29991231'
    AND (PRIM_CCBINS_ID is not null AND PRIM_CCBINS_ID <> '')
    AND (SCDY_CCBINS_ID is not null AND SCDY_CCBINS_ID <> '')) t

-- 追加主表为空的记录
INSERT INTO TABLE MID_CCBINS_CUR
SELECT
        CCBINS_ID,
        CCBINS_CHN_FULLNM,
        CCBINS_CHN_SHRTNM,
        CCBINS_ENG_FULLNM,
        CCBINS_ENG_SHRTNM,
        INST_STCD,
        null as PRIM_CCBINS_ID,
        null as CCBINSINRHIERSTCTP_CD,
        null as CCBINS_HIERSTC_NM,
        null as SCDY_CCBINS_ID,
        null as INSTMGTGRD_DSC,
        null as REL_SCN_DSC,
        null as LAST_UDT_DT_TM 
  FROM CT_T0651_CCBINS_INF_H
 WHERE P9_END_DATE = '29991231'
   AND (CCBINS_ID is null OR CCBINS_ID = '');
