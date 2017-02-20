use sor;

-- Hive建表脚本
-- CCBS帐户与卡整合表: MID_CARD_CUR

DROP TABLE IF EXISTS MID_CARD_CUR;

CREATE TABLE IF NOT EXISTS MID_CARD_CUR(
   -- 帐号
   SA_ACCT_NO                     string,
   -- 开户金额
   SA_OPAC_AMT                    string,
   -- 开户日期
   SA_OPAC_DT                     string,
   -- 记账机构
   SA_RCRD_INSTN_NO               string,
   -- 客户编号
   SA_CUST_NO                     string,
   -- 客户名称
   SA_CUST_NAME                   string,
   -- 联系人编号
   SA_CONNTR_NO                   string,
   -- 销户机构号
   SA_CACCT_INSTN_NO              string,
   -- 销户日期
   SA_CACCT_DT                    string,
   -- 开户柜员号
   SA_OPAC_TL_NO                  string,
   -- 销户柜员号
   SA_CACCT_TL_NO                 string,
   -- 理财卡一户通备付金签约标志
   SA_YHT_SIGN_FLG                string,
   -- 校园卡标志
   SA_XYK_FLG                     string,
   -- 支付限额控制标志
   SA_LMT_CTL_FLG                 string,
   -- 卡号
   SA_CARD_NO                     string,
   -- 综合理财标志
   SA_FS_STS                      string,
   -- 是否贷记卡指定还款帐号
   SA_CCR_FLG                     string,
   -- 卡号
   CR_CRD_NO                      string,
   -- 挂失日期
   CR_DL_DT                       string,
   -- 挂失状态
   CR_DL_STS                      string,
   -- 借贷别
   CR_DR_CR_COD                   string,
   -- 卡种代码
   CR_CRD_TYP_COD                 string,
   -- 卡状态
   CR_CRD_STS                     string,
   -- 客户编号
   CR_CUST_NO                     string,
   -- 营业单位代码
   CR_OPUN_COD                    string,
   -- 主附卡标志
   CR_MNSUB_DRP                   string,
   -- 主卡卡号
   CR_PCRD_NO                     string,
   -- 注销日期
   CR_CNCLC_DT                    string,
   -- 注销状态
   CR_CNCL_STS                    string,
   -- 更改日期
   CR_CHG_GRNTR_DT                string,
   -- 开卡日期
   CR_OPCR_DATE                   string,
   -- 发卡机构号
   CR_APP_NO                      string,
   -- 换卡日期
   CR_CRD_CHG_DT                  string,
   -- 换卡机构号
   CR_CHG_OPUN_COD                string
)
STORED AS ORC;

