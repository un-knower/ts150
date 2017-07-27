use sor;

# 中间表插入
# 机构表: MID_CCBINS_LEVEL_CUR

-- 有关联的数据插入
INSERT OVERWRITE TABLE MID_CCBINS_LEVEL_CUR
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
  LAST_UDT_DT_TM,
  size(lvl_array),
  lvl_array[0],
  lvl_array[1],
  lvl_array[2],
  lvl_array[3],
  lvl_array[4],
  lvl_array[5],
  lvl_array[6],
  lvl_array[7],
  lvl_array[8]
FROM (
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
         LAST_UDT_DT_TM,
         -- 机构树数组
         split(REL_SCN_DSC, '\\|') as lvl_array
      FROM MID_CCBINS_CUR
) a;
