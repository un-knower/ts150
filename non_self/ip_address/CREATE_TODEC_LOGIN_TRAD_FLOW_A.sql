use ts150;

-- 案件溯源Hive建表脚本
-- 登录流水表: TODEC_LOGIN_TRAD_FLOW_A

DROP TABLE IF EXISTS CT_TMP_TODEC_LOGIN_TRAD_FLOW_A;
DROP TABLE IF EXISTS CT_TODEC_LOGIN_TRAD_FLOW_A;

CREATE EXTERNAL TABLE IF NOT EXISTS CT_TMP_TODEC_LOGIN_TRAD_FLOW_A(
   QUERY_FLOW_NO                  string,
   CHANL_NO                       string,
   FLAT_TRAD_DATE                 string,
   CUST_NO                        string,
   CHANL_CUST_NO                  string,
   CUST_NM                        string,
   ACCT_NO                        string,
   ACCT_TYP                       string,
   ACCT_BRAN                      string,
   TRAD_NO                        string,
   TRAD_NM                        string,
   ITCD_NO                        string,
   FLAT_TRAD_TIME                 string,
   TRAD_BRAN                      string,
   RETURN_CODE                    string,
   RETURN_MSG                     string,
   mobile                         string,
   ip                             string,
   term_qry                       string,
   bios                           string,
   imei                           string,
   mac                            string,
   P9_DATA_DATE                   string
)
STORED AS TEXTFILE
LOCATION '/bigdata/input/TS150/case_trace/TODEC_LOGIN_TRAD_FLOW_A/';



CREATE TABLE IF NOT EXISTS CT_TODEC_LOGIN_TRAD_FLOW_A(
   QUERY_FLOW_NO                  string,
   CHANL_NO                       string,
   FLAT_TRAD_DATE                 string,
   CUST_NO                        string,
   CHANL_CUST_NO                  string,
   CUST_NM                        string,
   ACCT_NO                        string,
   ACCT_TYP                       string,
   ACCT_BRAN                      string,
   TRAD_NO                        string,
   TRAD_NM                        string,
   ITCD_NO                        string,
   FLAT_TRAD_TIME                 string,
   TRAD_BRAN                      string,
   RETURN_CODE                    string,
   RETURN_MSG                     string,
   mobile                         string,
   ip                             string,
   term_qry                       string,
   bios                           string,
   imei                           string,
   mac                            string,
   P9_DATA_DATE                   string
)
STORED AS ORC;

insert overwrite table CT_TODEC_LOGIN_TRAD_FLOW_A
select * from CT_TMP_TODEC_LOGIN_TRAD_FLOW_A;

