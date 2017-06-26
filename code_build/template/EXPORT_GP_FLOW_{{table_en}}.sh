#!/bin/sh

# 导出GP流水表 {{table_cn}}: {{table_en}}

source /home/ap/dip/appjob/shelljob/TS150/violate/base.sh

logdate_arg $*

IN_CUR_GP="base.{{table_en}}"
OUT_CUR_LOCAL="/home/ap/dip/file/archive/input/ts150/000000000/data/${date}/TS150_SAFEID_000000000_{{table_en}}_${date}_0001.dat"

$script_path/2_ready_data/export_gp_data.sh -d $log_date -t {{table_en}} -c "p9_data_date='$log_date'"
