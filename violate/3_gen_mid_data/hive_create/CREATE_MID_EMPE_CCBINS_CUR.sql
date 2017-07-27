use sor;

-- Hive建表脚本
-- 员工机构信息表: MID_EMPE_CCBINS_CUR

DROP TABLE IF EXISTS MID_EMPE_CCBINS_CUR;

CREATE TABLE IF NOT EXISTS MID_EMPE_CCBINS_CUR(
   -- 建行员工编号
   CCB_EMPID                      string,
   -- 性别代码
   GND_CD                         string,
   -- 用户姓名
   USR_NM                         string,
   -- 人力资源员工编号
   HMNRSC_EMPID                   string,
   -- 员工识别登录名
   EMPE_ID_LAND_NM                string,
   -- 证件类型代码
   CRDT_TPCD                      string,
   -- 证件号码
   CRDT_NO                        string,
   -- 用户状态代码
   USR_STCD                       string,
   -- 电话联系方式办公号码
   TELCTCMOD_WRK_NO               string,
   -- 电话联系方式手机号码
   TELCTCMOD_MBLPH_NO             string,
   -- 个人名称姓氏
   IDV_NM_SURNM                   string,
   -- 个人名称名
   IDV_NM_NM                      string,
   -- 详细地址
   DTL_ADR                        string,
   -- 所属机构编号
   BLNG_INSID                     string,
   -- 生效日期时间
   EFDT_TM                        string,
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
