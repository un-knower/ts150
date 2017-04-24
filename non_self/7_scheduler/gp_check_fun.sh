source /home/ap/dip_ts150/ts150_script/base.sh

# 生成的临时SQL，以当前时间与进程号为文件名
sqlfile=/home/ap/dip_ts150/ts150_script/monitor/sql/${curTime}_$$.sql

gp_pqsl()
{
   schema=${1:-app_siam}

   export HOME=/home/ap/dip
   source $HOME/.bash_profile

   
   if [ $schema == "app_siam" ]; then
      export PGPASSWORD=aca_siam_etl
      psql="psql -h 11.58.112.141 -d saldb -U aca_siam_etl"
   else
      export PGPASSWORD=password
      psql="psql -h 11.36.156.168 -d sordb -U aca_xk_user"
   fi
}

gp_partition_exists()
{
   table=${1:-p2log_cst_query}
   check_date=${2:-20010101}
   schema=${3:-app_siam}

   partition_table=${table}_1_prt_day${check_date}

   gp_pqsl $schema

   echo " set search_path to $schema;" > $sqlfile
   echo " \dt $partition_table;" >> $sqlfile

   result=`$psql -f $sqlfile | grep "$partition_table"  | wc -l `
   rm -f $sqlfile
   #echo $result

   if [ $result -ne 1 ]; then
      return 1
   fi

   return 0
}


