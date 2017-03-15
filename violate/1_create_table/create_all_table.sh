#!/bin/sh

for file in `ls hive_create/*.sql`; do
   echo $file
   beeline -f $file
   if [ $? -ne 0 ]; then
      echo "error, exit"
      exit -1
   fi
done
