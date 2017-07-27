#!/bin/sh

# 导出GP流水表 对私活期存款合约明细: T0182_TBSPTXN0_A

source /home/ap/dip/appjob/shelljob/TS150/violate/base.sh

logdate_arg $*

IN_CUR_GP="base.T0182_TBSPTXN0_A"
OUT_CUR_LOCAL="/home/ap/dip/file/archive/input/ts150/000000000/data/${date}/TS150_SAFEID_000000000_T0182_TBSPTXN0_A_${date}_0001.dat"

$script_path/2_ready_data/export_gp_data.sh -d $log_date -t T0182_TBSPTXN0_A -c "p9_data_date='$log_date'"
