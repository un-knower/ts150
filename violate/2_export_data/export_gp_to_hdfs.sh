#!/bin/sh
source /home/ap/dip_ts150/ts150_script/base.sh


valid(){
	if [ `echo ${log_date} | egrep "^2[0-9]{2}[0-1][0-9][0-3][0-9]$"` ];then
		echo "date ok"
	else
		echo "date error"
		usage
	fi
}

usage(){
	echo "命令行参数有误"
	echo "`basename $0` -d yyyymmdd -t tablename -c condition [-l gp accessid]"
	exit 1
}

arg(){
	gp_link=GP_SORDB_BASE
	OPT=no
	while getopts :l:d:t:c: OPTION
	do
		case $OPTION in
			l)gp_link=$OPTION
              ;;
            d)log_date=$OPTION
              OPT=yes
              ;;
            t)tablename=$OPTION
              OPT=yes
              ;;
            c)condition=$OPTION
              OPT=yes
              ;;
        esac
    done
    if [ "$OPT"="no" ];then
    	usage
    fi
    valid
}


hadoop_login

# 分隔符为'\001'导出数据
export_data_inc(){
	perl /home/ap/dip/bin/export_data.pl $gp_link $cur_filename $tablename "$condition" N "\001" Y utf8 1 N "" N ./ Y
}

# 上传数据到HDFS
upload_data(){
	hds_file=/bigdata/input/TS150/case_trace/${log_date}
	hadoop fs -CopyFromLocal $cur_filename $hds_file
}

arg $*
gp_link=GP_SORDB_BASE
cur_filename=/home/ap/dip/file/archive/input/ts150/000000000/data/${log_date}/TS150_SAFEID_000000000_${tablename}_${log_date}_0001.dat

# 导出数据到本地
export_data

# 查看文件是否存在并大小大于0
if [ ！-s $cur_filename ]; then
   echo "[Error] Export data is error"
   exit 1
fi

# 上传数据到hdfs
upload_data