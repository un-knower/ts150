#!/bin/sh

# 导出GP拉链表 对私活期存款合约: T0182_TBSPACN0_H

source /home/ap/dip_ts150/ts150_script/violate/base.sh

logdate_arg $*

IN_CUR_GP="base.T0182_TBSPACN0_H"
OUT_CUR_LOCAL="/home/ap/dip/file/archive/input/ts150/000000000/data/${date}/TS150_SAFEID_000000000_T0182_TBSPACN0_H_${date}_0001.dat"

$script_path/2_ready_data/export_gp_data.sh -d $log_date -t T0182_TBSPACN0_H -c "p9_start_date=DATE('$log_date')"
