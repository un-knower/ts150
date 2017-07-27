#!/bin/sh

# 导出GP流水表 交易支付痕迹: T1000_PAY_TRAD_FLOW_A

source /home/ap/dip/appjob/shelljob/TS150/violate/base.sh

logdate_arg $*

IN_CUR_GP="base.T1000_PAY_TRAD_FLOW_A"
OUT_CUR_LOCAL="/home/ap/dip/file/archive/input/ts150/000000000/data/${date}/TS150_SAFEID_000000000_T1000_PAY_TRAD_FLOW_A_${date}_0001.dat"

$script_path/2_ready_data/export_gp_data.sh -d $log_date -t T1000_PAY_TRAD_FLOW_A -c "p9_data_date='$log_date'"
