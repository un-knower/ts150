use sor;

# 中间表插入
# CCBS帐户与卡整合表: MID_CARD_CUR

-- 有关联的数据插入
INSERT OVERWRITE TABLE MID_CARD_CUR
SELECT
   -- 帐号
   SA_ACCT_NO,
   -- 开户金额
   SA_OPAC_AMT,
   -- 开户日期
   SA_OPAC_DT,
   -- 记账机构
   SA_RCRD_INSTN_NO,
   -- 客户编号
   SA_CUST_NO,
   -- 客户名称
   SA_CUST_NAME,
   -- 联系人编号
   SA_CONNTR_NO,
   -- 销户机构号
   SA_CACCT_INSTN_NO,
   -- 销户日期
   SA_CACCT_DT,
   -- 开户柜员号
   SA_OPAC_TL_NO,
   -- 销户柜员号
   SA_CACCT_TL_NO,
   -- 理财卡一户通备付金签约标志
   SA_YHT_SIGN_FLG,
   -- 校园卡标志
   SA_XYK_FLG,
   -- 支付限额控制标志
   SA_LMT_CTL_FLG,
   -- 卡号
   SA_CARD_NO,
   -- 综合理财标志
   SA_FS_STS,
   -- 是否贷记卡指定还款帐号
   SA_CCR_FLG,
   -- 卡号
   CR_CRD_NO,
   -- 挂失日期
   CR_DL_DT,
   -- 挂失状态
   CR_DL_STS,
   -- 借贷别
   CR_DR_CR_COD,
   -- 卡种代码
   CR_CRD_TYP_COD,
   -- 卡状态
   CR_CRD_STS,
   -- 客户编号
   CR_CUST_NO,
   -- 营业单位代码
   CR_OPUN_COD,
   -- 主附卡标志
   CR_MNSUB_DRP,
   -- 主卡卡号
   CR_PCRD_NO,
   -- 注销日期
   CR_CNCLC_DT,
   -- 注销状态
   CR_CNCL_STS,
   -- 更改日期
   CR_CHG_GRNTR_DT,
   -- 开卡日期
   CR_OPCR_DATE,
   -- 发卡机构号
   CR_APP_NO,
   -- 换卡日期
   CR_CRD_CHG_DT,
   -- 换卡机构号
   CR_CHG_OPUN_COD,
   -- 帐号
   SA_ACCT_NO,
   -- 开户金额
   SA_OPAC_AMT,
   -- 开户日期
   SA_OPAC_DT,
   -- 记账机构
   SA_RCRD_INSTN_NO,
   -- 客户编号
   SA_CUST_NO,
   -- 客户名称
   SA_CUST_NAME,
   -- 联系人编号
   SA_CONNTR_NO,
   -- 销户机构号
   SA_CACCT_INSTN_NO,
   -- 销户日期
   SA_CACCT_DT,
   -- 开户柜员号
   SA_OPAC_TL_NO,
   -- 销户柜员号
   SA_CACCT_TL_NO,
   -- 理财卡一户通备付金签约标志
   SA_YHT_SIGN_FLG,
   -- 校园卡标志
   SA_XYK_FLG,
   -- 支付限额控制标志
   SA_LMT_CTL_FLG,
   -- 卡号
   SA_CARD_NO,
   -- 综合理财标志
   SA_FS_STS,
   -- 是否贷记卡指定还款帐号
   SA_CCR_FLG,
   -- 卡号
   CR_CRD_NO,
   -- 挂失日期
   CR_DL_DT,
   -- 挂失状态
   CR_DL_STS,
   -- 借贷别
   CR_DR_CR_COD,
   -- 卡种代码
   CR_CRD_TYP_COD,
   -- 卡状态
   CR_CRD_STS,
   -- 客户编号
   CR_CUST_NO,
   -- 营业单位代码
   CR_OPUN_COD,
   -- 主附卡标志
   CR_MNSUB_DRP,
   -- 主卡卡号
   CR_PCRD_NO,
   -- 注销日期
   CR_CNCLC_DT,
   -- 注销状态
   CR_CNCL_STS,
   -- 更改日期
   CR_CHG_GRNTR_DT,
   -- 开卡日期
   CR_OPCR_DATE,
   -- 发卡机构号
   CR_APP_NO,
   -- 换卡日期
   CR_CRD_CHG_DT,
   -- 换卡机构号
   CR_CHG_OPUN_COD FROM
(SELECT
        -- 帐号
        SA_ACCT_NO,
        -- 开户金额
        SA_OPAC_AMT,
        -- 开户日期
        SA_OPAC_DT,
        -- 记账机构
        SA_RCRD_INSTN_NO,
        -- 客户编号
        SA_CUST_NO,
        -- 客户名称
        SA_CUST_NAME,
        -- 联系人编号
        SA_CONNTR_NO,
        -- 销户机构号
        SA_CACCT_INSTN_NO,
        -- 销户日期
        SA_CACCT_DT,
        -- 开户柜员号
        SA_OPAC_TL_NO,
        -- 销户柜员号
        SA_CACCT_TL_NO,
        -- 理财卡一户通备付金签约标志
        SA_YHT_SIGN_FLG,
        -- 校园卡标志
        SA_XYK_FLG,
        -- 支付限额控制标志
        SA_LMT_CTL_FLG,
        -- 卡号
        SA_CARD_NO,
        -- 综合理财标志
        SA_FS_STS,
        -- 是否贷记卡指定还款帐号
        SA_CCR_FLG
   FROM CT_TODDC_SAACNACN_H
  WHERE P9_END_DATE = '29991231'
    AND (SA_ACCT_NO is not null AND SA_ACCT_NO <> '')) t
INNER JOIN
(SELECT
        -- 卡号
        CR_CRD_NO,
        -- 挂失日期
        CR_DL_DT,
        -- 挂失状态
        CR_DL_STS,
        -- 借贷别
        CR_DR_CR_COD,
        -- 卡种代码
        CR_CRD_TYP_COD,
        -- 卡状态
        CR_CRD_STS,
        -- 客户编号
        CR_CUST_NO,
        -- 营业单位代码
        CR_OPUN_COD,
        -- 主附卡标志
        CR_MNSUB_DRP,
        -- 主卡卡号
        CR_PCRD_NO,
        -- 注销日期
        CR_CNCLC_DT,
        -- 注销状态
        CR_CNCL_STS,
        -- 更改日期
        CR_CHG_GRNTR_DT,
        -- 开卡日期
        CR_OPCR_DATE,
        -- 发卡机构号
        CR_APP_NO,
        -- 换卡日期
        CR_CRD_CHG_DT,
        -- 换卡机构号
        CR_CHG_OPUN_COD
   FROM CT_TODDC_CRCRDCRD_H
  WHERE P9_END_DATE = '29991231'
    AND (CR_CRD_NO is not null AND CR_CRD_NO <> '')) t

-- 追加从表为空的记录
INSERT INTO TABLE MID_CARD_CUR
SELECT
        SA_ACCT_NO,
        SA_OPAC_AMT,
        SA_OPAC_DT,
        SA_RCRD_INSTN_NO,
        SA_CUST_NO,
        SA_CUST_NAME,
        SA_CONNTR_NO,
        SA_CACCT_INSTN_NO,
        SA_CACCT_DT,
        SA_OPAC_TL_NO,
        SA_CACCT_TL_NO,
        SA_YHT_SIGN_FLG,
        SA_XYK_FLG,
        SA_LMT_CTL_FLG,
        SA_CARD_NO,
        SA_FS_STS,
        SA_CCR_FLG,
        null as CR_CRD_NO,
        null as CR_DL_DT,
        null as CR_DL_STS,
        null as CR_DR_CR_COD,
        null as CR_CRD_TYP_COD,
        null as CR_CRD_STS,
        null as CR_CUST_NO,
        null as CR_OPUN_COD,
        null as CR_MNSUB_DRP,
        null as CR_PCRD_NO,
        null as CR_CNCLC_DT,
        null as CR_CNCL_STS,
        null as CR_CHG_GRNTR_DT,
        null as CR_OPCR_DATE,
        null as CR_APP_NO,
        null as CR_CRD_CHG_DT,
        null as CR_CHG_OPUN_COD 
  FROM TODDC_CRCRDCRD_H
 WHERE P9_END_DATE = '29991231'
   AND (SA_ACCT_NO is null OR SA_ACCT_NO = '');
