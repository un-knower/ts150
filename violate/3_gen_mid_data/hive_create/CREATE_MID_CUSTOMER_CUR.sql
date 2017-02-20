use sor;

-- Hive建表脚本
-- 对私客户信息整合表，带原系统客户号: MID_CUSTOMER_CUR

DROP TABLE IF EXISTS MID_CUSTOMER_CUR;

CREATE TABLE IF NOT EXISTS MID_CUSTOMER_CUR(
   -- #源信息系统代码
   SRC_INF_STM_CD                 string,
   -- #源系统客户编号
   SRCSYS_CST_ID                  string,
   -- 客户编号
   CST_ID                         string,
   -- 个人法定名称
   IDV_LGL_NM                     string,
   -- 证件类型代码
   CRDT_TPCD                      string,
   -- 证件号码
   CRDT_NO                        string,
   -- 出生日期
   BRTH_DT                        string,
   -- 性别代码
   GND_CD                         string,
   -- 国籍代码
   NAT_CD                         string,
   -- 居住国代码
   RSDNC_NAT_CD                   string,
   -- 首选语言代码
   PREF_LNG_CD                    string,
   -- 户籍行政区划代码
   HSHLDRGST_ADIV_CD              string,
   -- 婚姻状况代码
   MAR_STTN_CD                    string,
   -- 子女状况代码
   CHL_STTN_CD                    string,
   -- 民族代码
   ETHNCT_CD                      string,
   -- 宗教信仰代码
   RLG_CD                         string,
   -- 政治面貌代码
   PLTCLPARTY_CD                  string,
   -- 生命周期状态代码
   LCS_CD                         string,
   -- 所属机构编号
   BLNG_INSID                     string,
   -- 建行员工标志
   CCB_EMPE_IND                   string,
   -- 计划财务有效客户标志
   PLN_FNC_EFCT_IND               string,
   -- 重要人士标志
   IMPT_PSNG_IND                  string,
   -- 潜力VIP标志
   PTNL_VIP_IND                   string,
   -- 特殊vip标志
   SPCLVIP_IND                    string,
   -- 系统评定客户等级代码
   STM_EVL_CST_GRD_CD             string,
   -- 手工评定客户等级代码
   MNUL_EVL_CST_GRD_CD            string,
   -- 私人银行客户等级代码
   PRVT_BNK_CST_GRD_CD            string,
   -- 私人银行签约客户标志
   PRVT_BNK_SIGN_CST_IND          string,
   -- 月收入金额
   MO_INCMAM                      string,
   -- 客户经理编号
   CSTMGR_ID                      string,
   -- 最佳联系电话
   BEST_CTC_TEL                   string,
   -- 企业高级管理人员标志
   ENTP_ADV_MGTPPL_IND            string,
   -- 企业实际控制人标志
   ENTP_ACT_CTRL_PSN_IND          string,
   -- 企业法人标志
   ENLGPS_IND                     string,
   -- 非居民标志
   NON_RSDNT_IND                  string,
   -- 工作单位性质代码
   WRK_UNIT_CHAR_CD               string
)
STORED AS ORC;

