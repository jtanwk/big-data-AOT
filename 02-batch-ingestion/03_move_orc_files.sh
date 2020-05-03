#!/bin/bash

# 03_move_orc_files.sh
# Moves newly-created ORC files in temp to directory for master dataset

# Get list of ORC files to move
files=`hdfs dfs -ls /warehouse/tablespace/managed/hive/jonathantan_temp | awk '!/^d/ {print $8}'`

# Rename with current date to avoid collisions
for f in $files; do
    mv $f $f_$(date +"%Y%m%d");
done

# Move to new directory
hdfs dfs -mv /warehouse/tablespace/managed/hive/jonathantan_temp/* /warehouse/tablespace/managed/hive/jonathantan_node_data
