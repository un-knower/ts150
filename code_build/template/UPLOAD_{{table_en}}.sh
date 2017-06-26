#!/bin/sh

# 上传{{table_cn}}: {{table_en}}

source /home/ap/dip/appjob/shelljob/TS150/violate/base.sh

hadoop_login

logdate_arg $*

IN_CUR_LOCAL="/home/ap/dip/file/archive/input/ts150/000000000/data/${date}/TS150_SAFEID_000000000_{{table_en}}_${date}_0001.dat"
OUT_CUR_HDFS="/bigdata/input/TS150/case_trace/{{table_en}}"

$script_path/2_ready_data/upload_to_hdfs.sh -d $log_date -t {{table_en}}  || exit 2
$script_path/2_ready_data/compress.sh -d $log_date -t {{table_en}} || exit 3
