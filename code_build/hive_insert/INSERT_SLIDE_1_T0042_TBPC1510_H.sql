--输入输出数据
--IN_CUR_HDFS="/bigdata/input/TS150/case_trace/T0042_TBPC1510_H"
--OUT_CUR_HIVE="sor.CT_T0042_TBPC1510_H_MID"
use sor;

-- Hive贴源数据处理
-- 客户联系位置信息: T0042_TBPC1510_H

-- 指定新数据日期分区位置
ALTER TABLE EXT_T0042_TBPC1510_H DROP IF EXISTS PARTITION(LOAD_DATE='${log_date}');
ALTER TABLE EXT_T0042_TBPC1510_H ADD PARTITION(LOAD_DATE='${log_date}')
            LOCATION 'hdfs://hacluster/bigdata/input/TS150/case_trace/T0042_TBPC1510_H/${log_date}/';

-- 备份贴源数据到ORC内部表
INSERT OVERWRITE TABLE INN_T0042_TBPC1510_H PARTITION(LOAD_DATE='${log_date}')
SELECT
    MULTI_TENANCY_ID,                       -- 多实体标识
    CST_ID,                                 -- 客户编号
    RCRD_EXPY_TMS,                          -- 记录失效时间戳
    CTC_INF_TPCD,                           -- 联系信息类型代码
    IDV_CTC_INF_SN,                         -- 个人联系信息序号
    RETPCD,                                 -- 关系类型代码
    TELCTCMOD_ITNL_DSTCNO,                  -- 电话联系方式国际区号
    TELCTCMOD_DMST_DSTCNO,                  -- 电话联系方式国内区号
    TELCTCMOD_NO,                           -- 电话联系方式号码
    TELCTCMOD_EXN_NO,                       -- 电话联系方式分机号码
    ZIPECD,                                 -- 邮政编码
    CTYRGON_CD,                             -- 国家地区代码
    PROV_ATNMSRGON_CD,                      -- 省自治区代码
    URBN_CD,                                -- 城市代码
    CNTYANDDSTC_CD,                         -- 区县代码
    DTL_ADR_CNTNT,                          -- 详细地址内容
    REL_STDT,                               -- 关系开始日期
    REL_EDDT,                               -- 关系结束日期
    CRT_INSID,                              -- 创建机构编号
    CRT_EMPID,                              -- 创建员工编号
    LAST_UDT_INSID,                         -- 最后更新机构编号
    LAST_UDT_EMPID,                         -- 最后更新员工编号
    CUR_STM_CRT_TMS,                        -- 当前系统创建时间戳
    CUR_STM_UDT_TMS,                        -- 当前系统更新时间戳
    LCL_YRMO_DAY,                           -- 本地年月日
    LCL_HR_GRD_SCND,                        -- 本地时分秒
    CRT_STM_NO,                             -- 创建系统号
    UDT_STM_NO,                             -- 更新系统号
    SRCSYS_CRT_TMS,                         -- 源系统创建时间戳
    SRCSYS_UDT_TMS,                         -- 源系统更新时间戳
    regexp_replace(P9_START_DATE, '-', ''), -- P9开始日期
    regexp_replace(P9_END_DATE, '-', '')    -- P9结束日期
  FROM EXT_T0042_TBPC1510_H
 WHERE LOAD_DATE='${log_date}';

-- 复制当天增量数据
INSERT OVERWRITE TABLE CT_T0042_TBPC1510_H_MID PARTITION(DATA_TYPE='${log_date}_INC')
SELECT
    CST_ID,                              -- 客户编号
    CTC_INF_TPCD,                        -- 联系信息类型代码
    TELCTCMOD_DMST_DSTCNO,               -- 电话联系方式国内区号
    TELCTCMOD_NO,                        -- 电话联系方式号码
    TELCTCMOD_EXN_NO,                    -- 电话联系方式分机号码
    ZIPECD,                              -- 邮政编码
    CTYRGON_CD,                          -- 国家地区代码
    PROV_ATNMSRGON_CD,                   -- 省自治区代码
    DTL_ADR_CNTNT,                       -- 详细地址内容
    P9_START_DATE,                       -- P9开始日期
    P9_END_DATE                          -- P9结束日期
  FROM INN_T0042_TBPC1510_H
 WHERE LOAD_DATE='${log_date}';
