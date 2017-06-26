--输入输出数据
--IN_CUR_HIVE="sor.INN_T0281_S11T1_BILL_DTL_A"
--IN_CUR_HIVE="sor.CT_T0281_TBB1PLT0_H"
--OUT_CUR_HIVE="sor.MID_CREDIT_BILL_DTL_A"

use sor;

-- 中间表插入
-- 信用卡账单表: MID_CREDIT_BILL_DTL_A
--    数据来源于：信用卡账单明细: INN_T0281_S11T1_BILL_DTL_A
--                信用卡: CT_T0281_TBB1PLT0_H

-- 有关联的数据插入
INSERT OVERWRITE TABLE MID_CREDIT_BILL_DTL_A PARTITION(P9_DATA_DATE='${log_date}')
SELECT
    ACPT_MRCH_NM,                        -- 受理商户名称
    ACPT_MRCH_NO,                        -- 受理商户号
    CCBINS_ID,                           -- 建行机构编号
    CRCRD_CARDNO,                        -- 信用卡卡号
    CRCRD_INR_TXN_CD,                    -- 信用卡内部交易码
    CRCRD_MNASBCRD_CD,                   -- 信用卡主附卡代码
    CRCRD_TXN_ACCENTR_AMT,               -- 信用卡交易入账金额
    CRCRDAR_ID,                          -- 信用卡合约编号
    CRCRDTXN_OPNT_ACCNO,                 -- 信用卡交易对手账号
    CRCRDTXN_OPNT_BKNM,                  -- 信用卡交易对手行名
    CRCRDTXN_OPNT_BRNO,                  -- 信用卡交易对手行号
    CRCRDTXN_OPNT_NM,                    -- 信用卡交易对手名称
    CRD_AHN_TXN_DT,                      -- 卡授权交易日期
    CRD_AHN_TXN_TM,                      -- 卡授权交易时间
    CST_ID,                              -- 客户编号
    DCC_AMT,                             -- DCC金额
    ED_CRD_PRTY_NM_ADR,                  -- 受卡方名称地址
    EDCRDMCHN_TMNL_IDR_CD,               -- 受卡机终端标识码
    SUB_MRCH_NM,                         -- 子商户名称
    SUB_MRCH_NO,                         -- 子商户号
    SYS_EVT_TRACE_ID,                    -- 全局事件跟踪号(流水号)
    TXN_NM_DSC,                          -- 交易名称描述
    TXN_TP_NM,                           -- 交易类型名称
    P9_DATA_DATE,                        -- P9数据日期
    CRCRD_CRDISU_BR_NO,                  -- 信用卡发卡分行号
    CRCRD_HSTCRD_CST_ID,                 -- 信用卡主卡客户编号
    CRCRD_AFCRD_CST_ID,                  -- 信用卡附属卡客户编号
    CRD_OPNCRD_DT,                       -- 卡片开卡日期
    CRD_CLS_DT,                          -- 卡片关闭日期
    CRD_GEN_DT,                          -- 卡片生成日期
    ACC_1_PD_ID,                         -- 账户一产品编号
    CRDISU_FST_CRCRDAR_ID,               -- 发卡第一信用卡合约编号
    ACC_2_PD_ID,                         -- 账户二产品编号
    CRDISU_SND_CRCRDAR_ID,               -- 发卡第二信用卡合约编号
    ACC_3_PD_ID,                         -- 账户三产品编号
    CRDISU_TRD_CRCRDAR_ID,               -- 发卡第三信用卡合约编号
    CRD_USE_ST_IND,                      -- 卡使用状态标志
    CRCRD_CMPN_EMPID,                    -- 信用卡营销员工编号
    CRCRD_CMPN_INSID                     -- 信用卡营销机构编号
  FROM
      (SELECT
           ACPT_MRCH_NM,                        -- 受理商户名称
           ACPT_MRCH_NO,                        -- 受理商户号
           CCBINS_ID,                           -- 建行机构编号
           CRCRD_CARDNO,                        -- 信用卡卡号
           CRCRD_INR_TXN_CD,                    -- 信用卡内部交易码
           CRCRD_MNASBCRD_CD,                   -- 信用卡主附卡代码
           CRCRD_TXN_ACCENTR_AMT,               -- 信用卡交易入账金额
           CRCRDAR_ID,                          -- 信用卡合约编号
           CRCRDTXN_OPNT_ACCNO,                 -- 信用卡交易对手账号
           CRCRDTXN_OPNT_BKNM,                  -- 信用卡交易对手行名
           CRCRDTXN_OPNT_BRNO,                  -- 信用卡交易对手行号
           CRCRDTXN_OPNT_NM,                    -- 信用卡交易对手名称
           CRD_AHN_TXN_DT,                      -- 卡授权交易日期
           CRD_AHN_TXN_TM,                      -- 卡授权交易时间
           CST_ID,                              -- 客户编号
           DCC_AMT,                             -- DCC金额
           ED_CRD_PRTY_NM_ADR,                  -- 受卡方名称地址
           EDCRDMCHN_TMNL_IDR_CD,               -- 受卡机终端标识码
           SUB_MRCH_NM,                         -- 子商户名称
           SUB_MRCH_NO,                         -- 子商户号
           SYS_EVT_TRACE_ID,                    -- 全局事件跟踪号(流水号)
           TXN_NM_DSC,                          -- 交易名称描述
           TXN_TP_NM,                           -- 交易类型名称
           P9_DATA_DATE                         -- P9数据日期
         FROM INN_T0281_S11T1_BILL_DTL_A
        WHERE P9_DATA_DATE='${log_date}'
          AND SYS_EVT_TRACE_ID is not null
          AND SYS_EVT_TRACE_ID <> '')
        ) t1
 INNER JOIN
       (SELECT
           CRCRD_CARDNO,                        -- 信用卡卡号
           CRCRD_CRDISU_BR_NO,                  -- 信用卡发卡分行号
           CRCRD_HSTCRD_CST_ID,                 -- 信用卡主卡客户编号
           CRCRD_AFCRD_CST_ID,                  -- 信用卡附属卡客户编号
           CRCRD_MNASBCRD_CD,                   -- 信用卡主附卡代码
           CRD_OPNCRD_DT,                       -- 卡片开卡日期
           CRD_CLS_DT,                          -- 卡片关闭日期
           CRD_GEN_DT,                          -- 卡片生成日期
           ACC_1_PD_ID,                         -- 账户一产品编号
           CRDISU_FST_CRCRDAR_ID,               -- 发卡第一信用卡合约编号
           ACC_2_PD_ID,                         -- 账户二产品编号
           CRDISU_SND_CRCRDAR_ID,               -- 发卡第二信用卡合约编号
           ACC_3_PD_ID,                         -- 账户三产品编号
           CRDISU_TRD_CRCRDAR_ID,               -- 发卡第三信用卡合约编号
           CRD_USE_ST_IND,                      -- 卡使用状态标志
           CRCRD_CMPN_EMPID,                    -- 信用卡营销员工编号
           CRCRD_CMPN_INSID                     -- 信用卡营销机构编号
         FROM CT_T0281_TBB1PLT0_H
        WHERE P9_END_DATE = '29991231'
          AND (CRCRD_CARDNO is not null
          AND CRCRD_CARDNO <> ''
        ) t2
   ON t1.SYS_EVT_TRACE_ID = t2.CRCRD_CARDNO;


-- 追加主表为空的记录
INSERT INTO TABLE MID_CREDIT_BILL_DTL_A PARTITION(P9_DATA_DATE='${log_date}')
SELECT
    ACPT_MRCH_NM,                        -- 受理商户名称
    ACPT_MRCH_NO,                        -- 受理商户号
    CCBINS_ID,                           -- 建行机构编号
    CRCRD_CARDNO,                        -- 信用卡卡号
    CRCRD_INR_TXN_CD,                    -- 信用卡内部交易码
    CRCRD_MNASBCRD_CD,                   -- 信用卡主附卡代码
    CRCRD_TXN_ACCENTR_AMT,               -- 信用卡交易入账金额
    CRCRDAR_ID,                          -- 信用卡合约编号
    CRCRDTXN_OPNT_ACCNO,                 -- 信用卡交易对手账号
    CRCRDTXN_OPNT_BKNM,                  -- 信用卡交易对手行名
    CRCRDTXN_OPNT_BRNO,                  -- 信用卡交易对手行号
    CRCRDTXN_OPNT_NM,                    -- 信用卡交易对手名称
    CRD_AHN_TXN_DT,                      -- 卡授权交易日期
    CRD_AHN_TXN_TM,                      -- 卡授权交易时间
    CST_ID,                              -- 客户编号
    DCC_AMT,                             -- DCC金额
    ED_CRD_PRTY_NM_ADR,                  -- 受卡方名称地址
    EDCRDMCHN_TMNL_IDR_CD,               -- 受卡机终端标识码
    SUB_MRCH_NM,                         -- 子商户名称
    SUB_MRCH_NO,                         -- 子商户号
    SYS_EVT_TRACE_ID,                    -- 全局事件跟踪号(流水号)
    TXN_NM_DSC,                          -- 交易名称描述
    TXN_TP_NM,                           -- 交易类型名称
    P9_DATA_DATE,                        -- P9数据日期
    null as CRCRD_CARDNO,                -- 信用卡卡号
    null as CRCRD_CRDISU_BR_NO,          -- 信用卡发卡分行号
    null as CRCRD_HSTCRD_CST_ID,         -- 信用卡主卡客户编号
    null as CRCRD_AFCRD_CST_ID,          -- 信用卡附属卡客户编号
    null as CRCRD_MNASBCRD_CD,           -- 信用卡主附卡代码
    null as CRD_OPNCRD_DT,               -- 卡片开卡日期
    null as CRD_CLS_DT,                  -- 卡片关闭日期
    null as CRD_GEN_DT,                  -- 卡片生成日期
    null as ACC_1_PD_ID,                 -- 账户一产品编号
    null as CRDISU_FST_CRCRDAR_ID,       -- 发卡第一信用卡合约编号
    null as ACC_2_PD_ID,                 -- 账户二产品编号
    null as CRDISU_SND_CRCRDAR_ID,       -- 发卡第二信用卡合约编号
    null as ACC_3_PD_ID,                 -- 账户三产品编号
    null as CRDISU_TRD_CRCRDAR_ID,       -- 发卡第三信用卡合约编号
    null as CRD_USE_ST_IND,              -- 卡使用状态标志
    null as CRCRD_CMPN_EMPID,            -- 信用卡营销员工编号
    null as CRCRD_CMPN_INSID             -- 信用卡营销机构编号
  FROM INN_T0281_S11T1_BILL_DTL_A
 WHERE P9_DATA_DATE='${log_date}'
   AND (SYS_EVT_TRACE_ID is null OR SYS_EVT_TRACE_ID = '');
