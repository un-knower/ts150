# unzip ip.zip

hadoop fs -mkdir -p /bigdata/input/TS150/case_trace/IP_AREA_FULL/
hadoop fs -mkdir -p /bigdata/input/TS150/case_trace/AREA_CODE/

hadoop fs -put -f ip.dat /bigdata/input/TS150/case_trace/IP_AREA_FULL/
hadoop fs -put -f area_code.dat /bigdata/input/TS150/case_trace/AREA_CODE/
hadoop fs -put -f ts150_hive_tools.jar /bigdata/common/function 
hadoop fs -put -f ip.txt /bigdata/common/function 


beeline -f ./create_ip_hive_table.sql