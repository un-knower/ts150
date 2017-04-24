# Hadoop状态检查脚本

source /home/ap/dip_ts150/ts150_script/ccb_risk_scoring/base.sh

# 生成的临时SQL，以当前时间与进程号为文件名
sqlfile=$script_path/7_scheduler/sql/${curTime}_$$.sql

# 判断HDFS文件是否已存在
hdfs_file_exists()
{
   # 参数1，HDFS目录名
   local path=$1
   local check_file_size=${2:-true}

   # 查HDFS文件数
   file_num=`hadoop fs -ls -R ${path} | wc -l` 
   # echo "file num:$file_num  $?"
   # echo $outlines | egrep "hive-staging|_COPYING_" 
   # 文件数为0，则退出1 
   if [ $file_num -eq 0 ]; then
      return 1
   fi

   # 检查正在处理标识，与文件大小
   outlines=`hadoop fs -ls -R ${path}` || return 2
   # echo "file[$outlines]"
   is_process=`echo $outlines | egrep "hive-staging|_COPYING_" | grep -v SUCC | wc -l`
   if [ $is_process -gt 0 ]; then
      return 3
   fi

   file_size_digit=`echo $outlines | awk '{size+=$5}END{print length(size)}'`
   # echo "file_size_digit:$file_size_digit"
   # 无正在处理标识，且文件总大小大于3位数字 
   if [ "$check_file_size" = "true" -a $file_size_digit -le 3 ]; then
      # 文件存在，返回0
      return 4
   fi

   return 0
}

# 判断Hive日期分区是否已存在
hive_partition_exists()
{
   # 参数1，Hive表名
   table=$1
   # 参数2，分区值，如导入日期
   partition_date=$2
   # 参数3，Hive数据库，默认为ts150库
   database=${3:-ts150}

   echo " use $database;" > $sqlfile
   echo " show partitions $table;" >> $sqlfile

   # 读取包含分区名称的行数
   outlines=`beeline -f $sqlfile | grep "$partition_date"`
   ret=$?
   rm -f $sqlfile

   partition_num=`echo $outlines | wc -l `
   # 行数为0，说明无该分区
   if [ $ret -ne 0 -o -$partition_num -eq 0 ]; then
      return 1
   fi

   local str1=${outlines%=*}
   local str2=${str1//|/}
   export partition_field=${str2// /}
   # echo "111:$partition_field"
   # echo $outlines 
   # 行数大于0，说明Hive分区已存在
   return 0
}

# 判断Hive分区存在，并已插入完成
hive_partition_over()
{
   # 参数1 表名
   table=`echo $1 | tr "[A-Z]" "[a-z]"`

   # 参数2 分区值名，如20170101
   partition_date=$2
   # 参数3 数据库名，默认ts150
   database=${3:-ts150}
   # 参数4 分区字段名，默认p9_data_date
   # partition_field=${4:-p9_data_date}
   local check_file_size=${4:-true}

   # 检查Hive分区是否存在
   hive_partition_exists $table $partition_date $database
   has_partition=$?
   # 分区不存在，退出
   if [ $has_partition -ne 0 ]; then
      return 1
   fi

   # 检查Hive分区在HDFS上的文件是否正在
   hdfs_file_exists /user/hive/warehouse/${database}.db/$table/${partition_field}=${partition_date}/ $check_file_size
   has_file=$?

   # HDFS上文件不存在，返回2
   if [ $has_file -ne 0 ]; then
      return 2
   fi

   return 0
}


unit_test()
{
   hdfs_file_exists /bigdata/input/TS150/p2log/20161201
   if [ $? -ne 0 ]; then
      echo "无文件或文件正在处理中。。。"
   fi
}
# unit_test
