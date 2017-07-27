use sor;

-- Hive�����ű�
-- pbcsϵͳ �޿���ز�ѯ������
--ILLEGAL_PBCS_PASSNUM �� ͨ�����š�����֤��ȫ ����֤��Ϣ


--(1)ͨ�����š�����֤��ȫ ����֤��Ϣ
DROP TABLE IF EXISTS ILLEGAL_PBCS_PASSNUM;

CREATE EXTERNAL TABLE IF NOT EXISTS ILLEGAL_PBCS_PASSNUM(
  QUE_DATE STRING,
  QUERY_ORG STRING,
  DEBIT_ACCT_NUM STRING,
  CUST_NO STRING,
  TX_OPERATOR STRING,

  CRDT_NO STRING,
  CARD_NO STRING
)
PARTITIONED BY (P9_DATA_DATE string)
STORED AS ORC;


--(2)��ȫ����������
DROP TABLE IF EXISTS ILLEGAL_PBCS_OPUN;

CREATE EXTERNAL TABLE IF NOT EXISTS ILLEGAL_PBCS_OPUN(
 QUE_DATE STRING,
  QUERY_ORG STRING,
  DEBIT_ACCT_NUM STRING,
  CUST_NO STRING,
  TX_OPERATOR STRING,

  CRDT_NO STRING,
  CARD_NO STRING,
  CR_OPUN_COD STRING
)
PARTITIONED BY (P9_DATA_DATE string)
STORED AS ORC;



--(3)��ȫͬ����ر�־
DROP TABLE IF EXISTS ILLEGAL_PBCS_SAME;

CREATE EXTERNAL TABLE IF NOT EXISTS ILLEGAL_PBCS_SAME(
  QUE_DATE STRING,
  QUERY_ORG STRING,
  DEBIT_ACCT_NUM STRING,
  CUST_NO STRING,
  TX_OPERATOR STRING,

  CRDT_NO STRING,
  CARD_NO STRING,
  CR_OPUN_COD STRING,
  SAME STRING
)
PARTITIONED BY (P9_DATA_DATE string)
STORED AS ORC;


--(4)��ѯ����صĿͻ���Ϣ
DROP TABLE IF EXISTS ILLEGAL_PBCS_NOSAME;

CREATE EXTERNAL TABLE IF NOT EXISTS ILLEGAL_PBCS_NOSAME(
  QUE_DATE STRING,
   TLR_ID STRING,
   CRDT_NO STRING,
   IS_SAME STRING
)
PARTITIONED BY (P9_DATA_DATE string)
STORED AS ORC;


--(5)��ѯ����صĲ�ѯ������ϸ
DROP TABLE IF EXISTS ILLEGAL_PBCS_NOSAME_A;

CREATE EXTERNAL TABLE IF NOT EXISTS ILLEGAL_PBCS_NOSAME_A(
  QUE_DATE STRING,
  TLR_ID STRING,
  QUERY_ORG STRING,
  DEBIT_ACCT_NUM STRING,
  CARD_NO STRING,
  CR_OPUN_COD STRING,
  CUST_NO STRING,
  CRDT_NO STRING
)
PARTITIONED BY (P9_DATA_DATE string)
STORED AS ORC;



--(6)��ѯ����ز�ѯ �� �����Ŷӻ��Ľ�����ϸ
DROP TABLE IF EXISTS ILLEGAL_PBCS_NOSAME_NOQUEUE;

CREATE EXTERNAL TABLE IF NOT EXISTS ILLEGAL_PBCS_NOSAME_NOQUEUE(
  QUE_DATE STRING,
  TLR_ID STRING,
  QUERY_ORG STRING,
  DEBIT_ACCT_NUM STRING,
  CARD_NO STRING,
  CR_OPUN_COD STRING,
  CUST_NO STRING,
  CRDT_NO STRING
)
PARTITIONED BY (P9_DATA_DATE string)
STORED AS ORC;


--(7)��ѯ����ز�ѯ �� �����Ŷӻ��Ľ�����ϸ
DROP TABLE IF EXISTS ILLEGAL_PBCS_OPUNNAME;

CREATE EXTERNAL TABLE IF NOT EXISTS ILLEGAL_PBCS_OPUNNAME(
  QUE_DATE STRING,
  TLR_ID STRING,
  QUERY_ORG STRING,
  DEBIT_ACCT_NUM STRING,
  CARD_NO STRING,
  CR_OPUN_COD STRING,
  CUST_NO STRING,
  CRDT_NO STRING,
  CR_OPUN_NM STRING
)
PARTITIONED BY (P9_DATA_DATE string)
STORED AS ORC;

--(8)���ӹ�Ա����֤��
DROP TABLE IF EXISTS ILLEGAL_PBCS_OPER_PASSNUM;

CREATE EXTERNAL TABLE IF NOT EXISTS ILLEGAL_PBCS_OPER_PASSNUM(
  QUE_DATE STRING,
  TLR_ID STRING,
  QUERY_ORG STRING,
  DEBIT_ACCT_NUM STRING,
  CARD_NO STRING,
  CR_OPUN_COD STRING,
  CUST_NO STRING,
  CRDT_NO STRING,
  CR_OPUN_NM STRING,
  CM_id_NO STRING
)
PARTITIONED BY (P9_DATA_DATE string)
STORED AS ORC;


--(9)��ȡԱ�����ƣ���Ա����������ż�������
DROP TABLE IF EXISTS ILLEGAL_PBCS_OPER_INFO;

CREATE EXTERNAL TABLE IF NOT EXISTS ILLEGAL_PBCS_OPER_INFO(
  QUE_DATE STRING,
  TLR_ID STRING,
  QUERY_ORG STRING,
  DEBIT_ACCT_NUM STRING,
  CARD_NO STRING,
  CR_OPUN_COD STRING,
  CUST_NO STRING,
  CRDT_NO STRING,
  CR_OPUN_NM STRING,
  CM_id_NO STRING,
  CM_OPR_NAME STRING,
  CM_OPUN_CODE STRING,
  CCBINS_CHN_SHRTNM STRING
)
PARTITIONED BY (P9_DATA_DATE string)
STORED AS ORC;


--(10)��ȡԱ������ĸ��׻���
DROP TABLE IF EXISTS ILLEGAL_PBCS_OPER_PARENT;

CREATE EXTERNAL TABLE IF NOT EXISTS ILLEGAL_PBCS_OPER_PARENT(
  QUE_DATE STRING,
  TLR_ID STRING,
  QUERY_ORG STRING,
  DEBIT_ACCT_NUM STRING,
  CARD_NO STRING,
  CR_OPUN_COD STRING,
  CUST_NO STRING,
  CRDT_NO STRING,
  CR_OPUN_NM STRING,
  CM_id_NO STRING,
  CM_OPR_NAME STRING,
  CM_OPUN_CODE STRING,
  CCBINS_CHN_SHRTNM STRING,
  PARENT_ID STRING,
  PARENT_CHN_SHRTNM STRING
)
PARTITIONED BY (P9_DATA_DATE string)
STORED AS ORC;



--(11���˿ͻ�û��ATM_������Ƚ��׵���ϸ��
DROP TABLE IF EXISTS ILLEGAL_PBCS_NOREFINE;

CREATE EXTERNAL TABLE IF NOT EXISTS ILLEGAL_PBCS_NOREFINE(
  QUE_DATE STRING,
  TLR_ID STRING,
  QUERY_ORG STRING,
  DEBIT_ACCT_NUM STRING,
  CARD_NO STRING,
  CR_OPUN_COD STRING,
  CUST_NO STRING,
  CRDT_NO STRING,
  CR_OPUN_NM STRING,
  CM_id_NO STRING,
  CM_OPR_NAME STRING,
  CM_OPUN_CODE STRING,
  CCBINS_CHN_SHRTNM STRING,
  PARENT_ID STRING,
  PARENT_CHN_SHRTNM STRING
)
PARTITIONED BY (P9_DATA_DATE string)
STORED AS ORC;



--(12)������ͳ�Ʋ�ѯ��
DROP TABLE IF EXISTS ILLEGAL_PBCS_NOREFINE_A;

CREATE EXTERNAL TABLE IF NOT EXISTS ILLEGAL_PBCS_NOREFINE_A(
  QUE_DATE STRING,
  TLR_ID STRING,
  CM_ID_NO STRING,
  CM_OPR_NAME STRING,
  QUERY_ORG STRING,
  CM_OPUN_CODE STRING,
  CCBINS_CHN_SHRTNM STRING,
  PARENT_ID STRING,
  PARENT_CHN_SHRTNM STRING,
  NUM STRING
)
PARTITIONED BY (P9_DATA_DATE string)
STORED AS ORC;