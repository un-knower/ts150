#!/bin/sh

# 上传对私活期存款合约: T0182_TBSPACN0_H

source /home/ap/dip/appjob/shelljob/TS150/violate/base.sh

hadoop_login

logdate_arg $*

IN_CUR_LOCAL="/home/ap/dip/file/archive/input/ts150/000000000/data/${date}/TS150_SAFEID_000000000_T0182_TBSPACN0_H_${date}_0001.dat"
OUT_CUR_HDFS="/bigdata/input/TS150/case_trace/T0182_TBSPACN0_H"

$script_path/2_ready_data/upload_to_hdfs.sh -d $log_date -t T0182_TBSPACN0_H  || exit 2
$script_path/2_ready_data/compress.sh -d $log_date -t T0182_TBSPACN0_H || exit 3
