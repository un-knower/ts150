use sor;

# Hive贴源数据处理
# 建行机构信息: T0651_CCBINS_INF_H

-- 指定新数据日期分区位置
ALTER TABLE EXT_T0651_CCBINS_INF_H DROP IF EXISTS PARTITION(LOAD_DATE='${log_date}');
ALTER TABLE EXT_T0651_CCBINS_INF_H ADD PARTITION (LOAD_DATE='${log_date}') LOCATION 'hdfs://hacluster/bigdata/input/case_trace/T0651_CCBINS_INF_H/${log_date}/';

-- 备份贴源数据到ORC内部表
INSERT OVERWRITE TABLE INN_T0651_CCBINS_INF_H PARTITION(LOAD_DATE='${log_date}')
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
   -- 建行机构名称语种代码
   CCBINS_NM_LNG_CD,
   -- 建行机构其他语种全称
   CCBINSOTHRLNGFULL_NM,
   -- 建行机构类型代码
   CCBINS_TPCD,
   -- 固定电话号码
   FIX_TELNO,
   -- 传真电话号码
   FAX_TELNO,
   -- 移动电话号码
   MOVE_TELNO,
   -- 行政区划代码
   ADIV_CD,
   -- 组织机构地域编号代码
   ORG_INST_RGON_ID_CD,
   -- 电子邮件地址内容
   EMAIL_ADR_CNTNT,
   -- Web地址
   WEB_ADR,
   -- QQ号码
   QQ_NO,
   -- MSN号码
   MSN_NO,
   -- 微博地址
   MCRBLG_ADR,
   -- 其他网络地址
   OTHR_NTW_ADR,
   -- 人民银行金融机构编码
   PBC_FNC_INST_ECD,
   -- 机构撤销后归属建行机构编号
   INSTUDOAFBLGCCBINS_ID,
   -- 机构状态代码
   INST_STCD,
   -- 机构核算层级代码
   INST_LDGR_HIER_CD,
   -- 工作日历编号
   WKCLD_ID,
   -- 经营主体属性代码
   OPSBJ_ATTRCD,
   -- 建行机构经营范围描述
   CCBINS_OPRT_SCOP_DSC,
   -- 金融许可证号
   FNC_LCNS_NO,
   -- 营业执照号码
   BSNLCNS_NO,
   -- 税务登记证号
   TAX_RGSCTF_NO,
   -- 组织机构代码证件号
   ORCD_CRDT_NO,
   -- 金融机构标识码
   FNC_INST_ID_CD,
   -- 支付系统银行行号
   PSBC,
   -- SWIFT号码
   SWIFT_NO,
   -- 银联卡地区代码
   UNNPY_CRD_RGON_CD,
   -- 人行交换号
   PBC_EXG_NO,
   -- 全国汇票机构号
   CTRYWD_DRFTBILL_INSNO,
   -- 全国联行号
   CTRYWD_BNKCD,
   -- 编制本票密押标志
   DVPPRMSNOTETSKY_IND,
   -- 国内信用证议付行标志
   DMST_LC_NGTBNK_IND,
   -- 国内信用证通知行标志
   DMST_LC_ADVSBNK_IND,
   -- 国内信用证开证行标志
   DMST_LC_ISUBNK_IND,
   -- 对公对私证券业务标志代码
   CORPPRVTSCRTBNS_INDCD,
   -- 财税库行联网业务标志
   TXBSBKNTWRKGBSN_IND,
   -- 大额支付业务标志
   BIGAMT_PY_BSN_IND,
   -- 小额支付业务标志
   MICR_PY_BSN_IND,
   -- 支票影像业务标志
   CHK_IMG_BSN_IND,
   -- 跨行网银业务标志
   INTRBNK_EBNKG_BSN_IND,
   -- 个人住房类贷款业务标志
   IDVHSCGYLOANBSN_IND,
   -- 个人消费类贷款业务标志
   IDVCNSMCGYLNBSN_IND,
   -- 个人经营类贷款业务标志
   IDVOPRTCGYLOANBSN_IND,
   -- 个人公积金贷款业务标志
   IDV_PRFDLN_BSN_IND,
   -- 住房资金业务标志
   HS_CPTLBSN_IND,
   -- 个人国际速汇业务银星标志
   IDVIEMTBSNSLVST_IND,
   -- 个人国际速汇业务西联标志
   IDVIEMTBSNWSTUN_IND,
   -- 西联汇款机构编码
   WSTUN_RMT_INST_ECD,
   -- 出入境服务中心标志
   EXETR_SVC_CNTR_IND,
   -- 个人贷款中心标志
   IDV_LOAN_CNTR_IND,
   -- 理财中心标志
   WLTHMGTCNTR_IND,
   -- 自助业务标志
   SLFSVC_BSN_IND,
   -- 财富中心标志
   WLTH_CNTR_IND,
   -- 提金点标志
   DRGDSPT_IND,
   -- 贵金属金库标志
   PM_TRSR_IND,
   -- 机构金融服务发展战略类型代码
   INSFCSVC_DPSTGTP_CD,
   -- 行别性质代码
   BNKCGY_CHAR_CD,
   -- 机构经济区域代码
   INST_ECN_RGON_CD,
   -- 专线电话号码
   SPPLN_TELNO,
   -- 理财柜台电话号码
   CHRTC_CNTER_TELNO,
   -- 建行机构行政层级代码
   CCBINS_ADMNHIER_CD,
   -- 责任中心类型代码
   RSPCNT_TPCD,
   -- 员工人数
   EMPLNUM,
   -- 机构负责人编号
   INST_PNP_ID,
   -- 立档单位标志
   DOC_UNIT_IND,
   -- 档案全宗编号
   FNDS_ID,
   -- 建行机构其他语种简称
   CCBINSOTHRLNGSHRT_NM,
   -- OPICS号
   OPICS_NO,
   -- POMS号
   POMS_NO,
   -- 建行机构描述
   CCBINS_DSC,
   -- 所在区域主要功能属性描述
   RGON_MAINFCN_ATTR_DSC,
   -- 所在区域次要功能属性描述
   RGON_MINRFCN_ATTR_DSC,
   -- 业务管理模式描述
   BSN_MGT_MODDSC,
   -- 对理财中心业务指导标志
   TOWLMGCNTRBSNGD_IND,
   -- 离附行标志
   OFFINBNK_IND,
   -- 穿墙标志
   NCLP_IND,
   -- 纳税人规模代码
   TAXPYR_SZ_CD,
   -- 企业规模代码
   ENTP_SZ_CD,
   -- 上市企业标志
   LSTD_ENTP_IND,
   -- 特殊经济区内企业标志
   SEACORP_IND,
   -- 进出口权标志
   IMPEXPRGT_IND,
   -- 企业环保达标标志
   ENTP_ENVPRT_CMPLN_IND,
   -- 建行机构总人数
   CCBINS_TOT_PNUM,
   -- 人行开户标志
   PBC_OPNACC_IND,
   -- 建行机构注册资本金额
   CCBINSRGSTCPTLFND_AMT,
   -- 建行机构注册资本币种代码
   CCBINSRGSTCPTLCCY_CD,
   -- 建行机构主营业务描述
   CCBINS_MAINBSN_DSC,
   -- 建行机构兼营业务描述
   CCBINS_MIX_BSN_DSC,
   -- #最后更新日期时间
   LAST_UDT_DT_TM,
   -- P9开始日期
   regexp_replace(P9_START_DATE, '-', ''),
   -- P9结束日期
   regexp_replace(P9_END_DATE, '-', ''),
   -- 建行机构成立日期
   regexp_replace(CCBINS_ESTB_DT, '-', ''),
   -- 机构变更日期
   regexp_replace(INST_MDF_DT, '-', ''),
   -- 机构撤销日期
   regexp_replace(INST_UDO_DT, '-', ''),
   -- 机构开始营业时间
   INST_STRT_OPRG_TM,
   -- 经营期限起始日期
   regexp_replace(OPRT_TRM_STDT, '-', ''),
   -- 经营期限结束日期
   regexp_replace(OPRT_TRM_EDDT, '-', ''),
   -- 开业日期
   regexp_replace(BOPR_DT, '-', ''),
   -- 承担费用标志
   CHRGTO_COSIND,
   -- 部门主责任类型代码
   DEPT_PRIM_RSPL_TPCD,
   -- 电票经营权限标志
   EBILL_OPRT_AHR_IND,
   -- 记账标志
   BOOKENTR_IND,
   -- 本部机构标志
   HDQRT_INST_IND,
   -- 部门属性代码
   DEPT_ATTRCD,
   -- 机构分账核算类型描述
   INST_SUBACC_LDGR_TPDS,
   -- HKICL分行编号
   HKICL_BR_ID,
   -- HKICL银行编号
   HKICL_BNK_ID
  FROM EXT_T0651_CCBINS_INF_H
 WHERE LOAD_DATE='${log_date}';
