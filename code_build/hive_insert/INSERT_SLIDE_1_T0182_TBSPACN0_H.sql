--输入输出数据
--IN_CUR_HDFS="/bigdata/input/TS150/case_trace/T0182_TBSPACN0_H"
--OUT_CUR_HIVE="sor.CT_T0182_TBSPACN0_H_MID"
use sor;

-- Hive贴源数据处理
-- 对私活期存款合约: T0182_TBSPACN0_H

-- 指定新数据日期分区位置
ALTER TABLE EXT_T0182_TBSPACN0_H DROP IF EXISTS PARTITION(LOAD_DATE='${log_date}');
ALTER TABLE EXT_T0182_TBSPACN0_H ADD PARTITION(LOAD_DATE='${log_date}')
            LOCATION 'hdfs://hacluster/bigdata/input/TS150/case_trace/T0182_TBSPACN0_H/${log_date}/';

-- 备份贴源数据到ORC内部表
INSERT OVERWRITE TABLE INN_T0182_TBSPACN0_H PARTITION(LOAD_DATE='${log_date}')
SELECT
    MULTI_TENANCY_ID,                       -- 多实体标识
    APL_PRTN_NO,                            -- 应用分区号
    CST_ACCNO,                              -- 客户账号
    DEPAR_SN,                               -- 存款合约序号
    DEPAR_ID,                               -- 存款合约编号
    IDCST_ACCNO_NM,                         -- 个人客户账号名称
    ASPD_ASB_VNO,                           -- 可售产品装配版本号
    ASPD_ID,                                -- 可售产品编号
    FNDS_USE_CD,                            -- 资金用途代码
    CST_ID,                                 -- 客户编号
    CSTMGR_USR_ID,                          -- 客户经理用户编号
    PRVT_DEPAR_STCD,                        -- 对私存款合约状态代码
    SEAL_CRD_CFINF_ID,                      -- 印鉴卡认证信息编号
    regexp_replace(OPNACC_DT, '-', ''),     -- 开户日期
    OPNACC_CHNL_ID,                         -- 开户渠道编号
    OPNACC_EMPID,                           -- 开户员工编号
    DPBKINNO,                               -- 开户机构编号
    LDGR_INSID,                             -- 核算机构编号
    OPNACC_CHNL_TPCD,                       -- 开户渠道类型代码
    CNCLACCT_CHNL_TPCD,                     -- 销户渠道类型代码
    regexp_replace(CNCLACCT_DT, '-', ''),   -- 销户日期
    CNCLACCT_CHNL_ID,                       -- 销户渠道编号
    CNCLACCT_EMPID,                         -- 销户员工编号
    CNCLACCT_INSID,                         -- 销户机构编号
    RCVPY_CTRL_STCD,                        -- 收付控制状态代码
    ORI_RCVPY_CTRL_STCD,                    -- 原收付控制状态代码
    SLP_STCD,                               -- 睡眠状态代码
    regexp_replace(SLP_DT, '-', ''),        -- 睡眠日期
    DEP_TFR_SLP_TPCD,                       -- 存款转睡眠类型代码
    SLP_SWTBCK_APRV_STCD,                   -- 睡眠转回审批状态代码
    DEP_ALRDY_PRT_DTL_NUM,                  -- 存款已打印明细数
    DEP_ALRDY_PRT_PG_NUM,                   -- 存款已打印页数
    ALRDY_PRT_LNUM,                         -- 已打印行数
    DEP_VERF_MTDCD,                         -- 存款校验方式代码
    FCS_SIGN_NUM,                           -- 关注签约数量
    PD_SALE_FTA_CD,                         -- 产品销售自贸区代码
    DEP_OPNACC_CRDT_TPCD,                   -- 存款开户证件类型代码
    DEP_BKLTNO,                             -- 存款存折册号
    regexp_replace(PSBK_LOSSRGST_DT, '-', ''),-- 存折挂失日期
    DEP_PSBK_STCD,                          -- 存款存折状态代码
    PRVTDEPSIGNTPLIST_VAL,                  -- 对私存款签约类型列表值
    LNCRDREPYACCNOSIGNNUM,                  -- 贷记卡还款账号签约数量
    IDV_FRNCY_ACCHAR_CD,                    -- 个人外币账户性质代码
    CST_PINOFFSET_VAL,                      -- 客户PINOFFSET值
    DEP_PSWD_STCD,                          -- 存款密码状态代码
    DVV_SAFE_INDX_ID,                       -- DVV安全索引编号
    DEP_ACCHAR_CD,                          -- 存款账户性质代码
    CSHEX_CHAR_CD,                          -- 钞汇性质代码
    PSWD_WRG_CNT,                           -- 密码出错次数
    regexp_replace(PSWD_LOSSRGST_DT, '-', ''),-- 密码挂失日期
    SUSP_4_BIT_PSWD_IND,                    -- 疑似4位密码标志
    DBCRD_AR_ID,                            -- 借记卡合约编号
    DBCRD_CARDNO,                           -- 借记卡卡号
    DEP_PSBK_PRT_NO,                        -- 存款存折印刷号
    UNVSDEP_SCOP_CD,                        -- 通存范围代码
    UNVSWD_SCOP_CD,                         -- 通兑范围代码
    CST_ACCNM_VLD_IND,                      -- 客户户名验证标志
    INTAR_IND,                              -- 计息标志
    OPNACC_AMT,                             -- 开户金额
    CUR_DTL_MAX_SN,                         -- 当前明细最大序号
    regexp_replace(CUR_DTL_LAST_TXN_DT, '-', ''),-- 当前明细最后交易日期
    INTDYIDVCACNYENCMTAMT,                  -- 当日个人活期账户人民币取现金额
    INTDYIDVCAUSDENCMTAMT,                  -- 当日个人活期账户美元取现金额
    INTDY_INT_DTL_SN,                       -- 当日首笔明细序号
    DEP_OPNACC_CRDT_NO,                     -- 存款开户证件号码
    ACC_RSK_GRD_TPCD,                       -- 账户风险等级类型代码
    SETL_ACC_CLCD,                          -- 结算账户分类代码
    regexp_replace(LAST_UDT_OPRGDAY, '-', ''),-- 最后更新营业日
    TMS,                                    -- 时间戳
    regexp_replace(P9_START_DATE, '-', ''), -- P9开始日期
    regexp_replace(P9_END_DATE, '-', '')    -- P9结束日期
  FROM EXT_T0182_TBSPACN0_H
 WHERE LOAD_DATE='${log_date}';

-- 复制当天增量数据
INSERT OVERWRITE TABLE CT_T0182_TBSPACN0_H_MID PARTITION(DATA_TYPE='${log_date}_INC')
SELECT
    CST_ACCNO,                           -- 客户账号
    DEPAR_ID,                            -- 存款合约编号
    IDCST_ACCNO_NM,                      -- 个人客户账号名称
    CST_ID,                              -- 客户编号
    CSTMGR_USR_ID,                       -- 客户经理用户编号
    PRVT_DEPAR_STCD,                     -- 对私存款合约状态代码
    OPNACC_DT,                           -- 开户日期
    OPNACC_CHNL_ID,                      -- 开户渠道编号
    OPNACC_EMPID,                        -- 开户员工编号
    DPBKINNO,                            -- 开户机构编号
    CNCLACCT_DT,                         -- 销户日期
    CNCLACCT_CHNL_ID,                    -- 销户渠道编号
    CNCLACCT_EMPID,                      -- 销户员工编号
    CNCLACCT_INSID,                      -- 销户机构编号
    SLP_STCD,                            -- 睡眠状态代码
    SLP_DT,                              -- 睡眠日期
    DEP_TFR_SLP_TPCD,                    -- 存款转睡眠类型代码
    DEP_OPNACC_CRDT_TPCD,                -- 存款开户证件类型代码
    PSBK_LOSSRGST_DT,                    -- 存折挂失日期
    DEP_PSBK_STCD,                       -- 存款存折状态代码
    PSWD_WRG_CNT,                        -- 密码出错次数
    PSWD_LOSSRGST_DT,                    -- 密码挂失日期
    DBCRD_AR_ID,                         -- 借记卡合约编号
    DBCRD_CARDNO,                        -- 借记卡卡号
    OPNACC_AMT,                          -- 开户金额
    DEP_OPNACC_CRDT_NO,                  -- 存款开户证件号码
    SETL_ACC_CLCD,                       -- 结算账户分类代码
    P9_START_DATE,                       -- P9开始日期
    P9_END_DATE                          -- P9结束日期
  FROM INN_T0182_TBSPACN0_H
 WHERE LOAD_DATE='${log_date}';
