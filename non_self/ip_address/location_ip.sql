use sor;

set role admin;
add jar hdfs:///bigdata/common/function/ts150_hive_tools.jar;
add jar hdfs:///bigdata/common/function/ip.txt;

create temporary function ts150_ip2num as 'com.ccb.ts150.hive_udf.Ip2Num_UDF';
create temporary function ts150_CalcDistance as 'com.ccb.ts150.hive_udf.CalcDistance_UDF';
create temporary function ts150_iplocation as 'com.ccb.ts150.hive_udf.GetIpLocation_UDF';
create temporary function ts150_iplocation_all as 'com.ccb.ts150.hive_udf.GetIpLocation_UDTF';


drop table ip_test;
create table ip_test as
select /*+ MAPJOIN(ip_area_full) */
       t1.ip, t2.area_code, t2.continentName, t2.country, t2.sure_address, t2.longitude, t2.latitude
  from CT_TODEC_LOGIN_TRAD_FLOW_A t1
 inner join ip_area_full t2
 where ts150_ip2num(t1.ip) >= t2.num_start_ip
   and ts150_ip2num(t1.ip) <  t2.num_end_ip;

select regexp_extract(sure_address, "(柘荣)", 1)
 from ip_area_full 
 limit 10;


set mapred.reduce.tasks=1000;
create table a_1 as 
select * from CT_TODEC_LOGIN_TRAD_FLOW_A 
distribute by ip; 


select cust_nm, ip, ts150_iplocation(ip, 1), ts150_iplocation(ip, 3), ts150_iplocation(ip, 4)
from CT_TODEC_LOGIN_TRAD_FLOW_A limit 10;

select cust_nm, ip, ts150_iplocation(ip, 1), ts150_iplocation(ip, 3), ts150_iplocation(ip, 4)
from CT_TODEC_LOGIN_TRAD_FLOW_A 
lateral view ts150_iplocation_all(ip) location as continentName, area, country, area_code, sure_address,
             longitude, latitude, ip_address_type
limit 10;

