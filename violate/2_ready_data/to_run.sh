#!/bin/sh

# source /home/ap/dip_ts150/ts150_script/base.sh
source /home/ap/dip/appjob/shelljob/TS150/p2log/base.sh

./export_gp_to_hdfs.sh -d $curDate -t T0651_CCBINS_INF_H -c "1=1" -l TS150_GP_PL3
