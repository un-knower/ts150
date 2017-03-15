use sor;

# 中间表插入
# 员工机构信息表: MID_EMPE_CCBINS_CUR

-- 有关联的数据插入
INSERT OVERWRITE TABLE MID_EMPE_CCBINS_CUR
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
   -- 用户状态代码
   USR_STCD,
   -- 电话联系方式办公号码
   TELCTCMOD_WRK_NO,
   -- 电话联系方式手机号码
   TELCTCMOD_MBLPH_NO,
   -- 个人名称姓氏
   IDV_NM_SURNM,
   -- 个人名称名
   IDV_NM_NM,
   -- 详细地址
   DTL_ADR,
   -- 所属机构编号
   BLNG_INSID,
   -- 生效日期时间
   EFDT_TM,
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
        -- 用户状态代码
        USR_STCD,
        -- 电话联系方式办公号码
        TELCTCMOD_WRK_NO,
        -- 电话联系方式手机号码
        TELCTCMOD_MBLPH_NO,
        -- 个人名称姓氏
        IDV_NM_SURNM,
        -- 个人名称名
        IDV_NM_NM,
        -- 详细地址
        DTL_ADR,
        -- 所属机构编号
        BLNG_INSID,
        -- 生效日期时间
        EFDT_TM
   FROM CT_T0861_EMPE_H
  WHERE P9_END_DATE = '29991231'
    AND (CCB_EMPID is not null AND CCB_EMPID <> '')) t1
INNER JOIN
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
        LAST_UDT_DT_TM
   FROM MID_CCBINS_CUR
  WHERE (CCBINS_ID is not null AND CCBINS_ID <> '')) t2
ON t1.BLNG_INSID = t2.CCBINS_ID;


-- 追加主表为空的记录
INSERT INTO TABLE MID_EMPE_CCBINS_CUR
SELECT
        CCB_EMPID,
        GND_CD,
        USR_NM,
        HMNRSC_EMPID,
        EMPE_ID_LAND_NM,
        CRDT_TPCD,
        CRDT_NO,
        USR_STCD,
        TELCTCMOD_WRK_NO,
        TELCTCMOD_MBLPH_NO,
        IDV_NM_SURNM,
        IDV_NM_NM,
        DTL_ADR,
        BLNG_INSID,
        EFDT_TM,
        null as CCBINS_ID,
        null as CCBINS_CHN_FULLNM,
        null as CCBINS_CHN_SHRTNM,
        null as CCBINS_ENG_FULLNM,
        null as CCBINS_ENG_SHRTNM,
        null as INST_STCD,
        null as PRIM_CCBINS_ID,
        null as CCBINSINRHIERSTCTP_CD,
        null as CCBINS_HIERSTC_NM,
        null as SCDY_CCBINS_ID,
        null as INSTMGTGRD_DSC,
        null as REL_SCN_DSC,
        null as LAST_UDT_DT_TM 
  FROM CT_T0861_EMPE_H
 WHERE P9_END_DATE = '29991231'
   AND (CCB_EMPID is null OR CCB_EMPID = '');
