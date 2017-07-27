use sor;

DROP TABLE IF EXISTS ext_ip_area_full;

CREATE TABLE IF NOT EXISTS ext_ip_area_full
(
   str_start_ip           string,
   str_end_ip             string,
   num_start_ip           bigint,
   num_end_ip             bigint,
   continentName          string,
   area                   string,
   country                string,
   original_address       string,
   area_code              string,
   sure_address           string,
   longitude              double,
   latitude               double
)
STORED AS TEXTFILE
LOCATION '/bigdata/input/TS150/case_trace/IP_AREA_FULL/';


DROP TABLE IF EXISTS ip_area_full;

CREATE TABLE IF NOT EXISTS ip_area_full
(
   str_start_ip           string,
   str_end_ip             string,
   num_start_ip           bigint,
   num_end_ip             bigint,
   continentName          string,
   area                   string,
   country                string,
   original_address       string,
   area_code              string,
   sure_address           string,
   longitude              double,
   latitude               double
)
STORED AS ORC;


-- hadoop fs -mkdir -p /bigdata/input/TS150/case_trace/IP_AREA_FULL/
-- unzip ip.zip
-- hadoop fs -put -f ip.dat /bigdata/input/TS150/case_trace/IP_AREA_FULL/
-- select * from ext_ip_area_full limit 3;
-- hadoop fs -du -h /user/hive/warehouse/sor.db/ip_area_full
-- 5.0 M  /user/hive/warehouse/sor.db/ip_area_full/000000_0

INSERT OVERWRITE TABLE ip_area_full
SELECT * FROM ext_ip_area_full;


DROP TABLE IF EXISTS ext_area_code;

CREATE TABLE IF NOT EXISTS ext_area_code
(
   area_code              string,
   parent_code            string,
   level                  int,
   area_name              string,
   longitude              double,
   latitude               double
)
STORED AS TEXTFILE
LOCATION '/bigdata/input/TS150/case_trace/AREA_CODE/';


DROP TABLE IF EXISTS area_code;

CREATE TABLE IF NOT EXISTS area_code
(
   area_code              string,
   parent_code            string,
   level                  int,
   area_name              string,
   longitude              double,
   latitude               double
)
STORED AS ORC;


-- hadoop fs -mkdir -p /bigdata/input/TS150/case_trace/AREA_CODE/
-- hadoop fs -put -f area_code.dat /bigdata/input/TS150/case_trace/AREA_CODE/
-- select * from ext_area_code limit 3;
-- select * from area_code limit 3;
-- hadoop fs -du -h /user/hive/warehouse/sor.db/area_code
-- 76.1 K  /user/hive/warehouse/sor.db/area_code/000000_0

INSERT OVERWRITE TABLE area_code
SELECT * FROM ext_area_code;

-- hadoop fs -put -f ts150_hive_tools.jar /bigdata/common/function 

set role admin;
add jar hdfs:///bigdata/common/function/ts150_hive_tools.jar;

create temporary function ts150_ip2num as 'com.ccb.ts150.hive_udf.Ip2Num_UDF';
create temporary function ts150_CalcDistance as 'com.ccb.ts150.hive_udf.CalcDistance_UDF';

select area_code, area_name, ts150_CalcDistance(119.900609, 27.233933, longitude, latitude) from area_code limit 2;


DROP TABLE IF EXISTS uniq_area_code;

CREATE TABLE IF NOT EXISTS uniq_area_code
(
   area_code              string,
   parent_code            string,
   level                  int,
   area_name              string,
   longitude              double,
   latitude               double
)
STORED AS ORC;
