use sor;

-- Hive建表脚本
-- 机构表: MID_CCBINS_CUR

DROP TABLE IF EXISTS MID_CCBINS_CUR;

CREATE TABLE IF NOT EXISTS MID_CCBINS_CUR(
   -- 建行机构编号
   CCBINS_ID                      string,
   -- 建行机构中文全称
   CCBINS_CHN_FULLNM              string,
   -- 建行机构中文简称
   CCBINS_CHN_SHRTNM              string,
   -- 建行机构英文全称
   CCBINS_ENG_FULLNM              string,
   -- 建行机构英文简称
   CCBINS_ENG_SHRTNM              string,
   -- 机构状态代码
   INST_STCD                      string,
   -- 主建行机构编号
   PRIM_CCBINS_ID                 string,
   -- 建行机构内部层级结构类型代码
   CCBINSINRHIERSTCTP_CD          string,
   -- 建行机构层级结构名称
   CCBINS_HIERSTC_NM              string,
   -- 从建行机构编号
   SCDY_CCBINS_ID                 string,
   -- 机构管理等级描述
   INSTMGTGRD_DSC                 string,
   -- 关系场景描述
   REL_SCN_DSC                    string,
   -- #最后更新日期时间
   LAST_UDT_DT_TM                 string
)
STORED AS ORC;
