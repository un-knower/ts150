source /home/ap/dip_ts150/ts150_script/base.sh

local_file_exists()
{
   path=$1

   if [ ! -f $path ]; then
      return 1
   fi

   return 0
}


local_file_num()
{
   path=$1
   name=$2
   if [ $# -ne 2 ]; then
      return 1
   fi

   if [ ! -d $path ]; then
      return 2
   fi

   #ls -rtl $path/$name

   file_num=`ls -rtl $path/$name | wc -l`

   return 0
}

unit_test()
{
}
unit_test
