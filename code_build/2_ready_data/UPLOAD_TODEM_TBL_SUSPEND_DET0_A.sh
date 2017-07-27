#!/bin/sh

# 上传挂起交易明细表: TODEM_TBL_SUSPEND_DET0_A

source /home/ap/dip/appjob/shelljob/TS150/violate/base.sh

hadoop_login

logdate_arg $*

IN_CUR_LOCAL="/home/ap/dip/file/archive/input/ts150/000000000/data/${date}/TS150_SAFEID_000000000_TODEM_TBL_SUSPEND_DET0_A_${date}_0001.dat"
OUT_CUR_HDFS="/bigdata/input/TS150/case_trace/TODEM_TBL_SUSPEND_DET0_A"

$script_path/2_ready_data/upload_to_hdfs.sh -d $log_date -t TODEM_TBL_SUSPEND_DET0_A  || exit 2
$script_path/2_ready_data/compress.sh -d $log_date -t TODEM_TBL_SUSPEND_DET0_A || exit 3