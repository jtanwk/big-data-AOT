#!/bin/bash

# 01_download_bulk_data.sh
# Downloads data from https://afb.plenar.io/data-sets/chicago-complete

# Calculates correct time frame for month-specific URL
lastday=`cal $(date --date='-1 month' +"%m %Y") | awk 'NF {DAYS = $NF}; END {print DAYS}'`
date=$(date +"%Y-%m-01-to-%Y-%m-${lastday}")
wget https://s3.amazonaws.com/aot-tarballs/chicago-complete.monthly.${date}.tar

# Decompress data
file=`chicago-complete.monthly.${date}.tar`
tar -xf $file
cd ${file%.tar}
gunzip data.csv.gz

# Put data in HDFS
mv data.csv ${file%.tar}_data.csv
hdfs dfs -put ${file%.tar}_data.csv /jonathantan/data/new_data/${file%.tar}.csv

# Cleanup
cd ..
rm -r ${file%.tar}
rm $file
