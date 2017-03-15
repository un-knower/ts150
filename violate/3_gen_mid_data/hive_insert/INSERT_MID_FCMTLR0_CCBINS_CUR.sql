use sor;

-- 中间表插入
-- 柜员机构信息表: MID_FCMTLR0_CCBINS_CUR

-- 有关联的数据插入
INSERT OVERWRITE TABLE MID_FCMTLR0_CCBINS_CUR
SELECT
   -- 操作员号
   CM_OPR_NO,
   -- 操作员姓名
   CM_OPR_NAME,
   -- 身份证号码
   CM_ID_NO,
   -- 柜员备注
   CM_TLR_RMRK,
   -- 柜员状态
   CM_TLR_STS,
   -- 所属机构号
   CM_OPUN_CODE,
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
        -- 操作员号
        CM_OPR_NO,
        -- 操作员姓名
        CM_OPR_NAME,
        -- 身份证号码
        CM_ID_NO,
        -- 柜员备注
        CM_TLR_RMRK,
        -- 柜员状态
        CM_TLR_STS,
        -- 所属机构号
        CM_OPUN_CODE
   FROM CT_TODDC_FCMTLR0_H
  WHERE P9_END_DATE = '29991231'
    AND (CM_OPR_NO is not null AND CM_OPR_NO <> '')
    AND (CM_OPUN_CODE is not null AND CM_OPUN_CODE <> '')) t1
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
   FROM CT_MID_CCBINS_CUR
  WHERE P9_END_DATE = '29991231'
    AND (CCBINS_ID is not null AND CCBINS_ID <> '')) t2
ON t1.CM_OPUN_CODE = t2.CCBINS_ID;


-- 追加主表为空的记录
INSERT INTO TABLE MID_FCMTLR0_CCBINS_CUR
SELECT
        CM_OPR_NO,
        CM_OPR_NAME,
        CM_ID_NO,
        CM_TLR_RMRK,
        CM_TLR_STS,
        CM_OPUN_CODE,
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
  FROM CT_TODDC_FCMTLR0_H
 WHERE P9_END_DATE = '29991231'
   AND ((CM_OPR_NO is null OR CM_OPR_NO = '')
         OR 
        (CM_OPUN_CODE is null OR CM_OPUN_CODE = ''));
