#!/bin/bash

# 01_getData.sh
# Downloads data from https://afb.plenar.io/data-sets/chicago-complete

# Specify dates to download
declare -a dates=(
    "2018-10-01-to-2018-10-31"
    "2018-11-01-to-2018-10-30"
    "2018-12-01-to-2018-12-31"
    "2019-01-01-to-2019-01-31"
    "2019-02-01-to-2019-02-28"
    "2019-03-01-to-2019-03-31"
    "2019-04-01-to-2019-04-30"
    "2019-05-01-to-2019-05-31"
    "2019-06-01-to-2019-06-30"
    "2019-07-01-to-2019-07-31"
    "2019-08-01-to-2019-08-31"
    "2019-09-01-to-2019-09-30"
)

# Get data
for date in ${dates[@]}; do
    echo "Downloading data from $date"
    wget https://s3.amazonaws.com/aot-tarballs/chicago-complete.monthly.${date}.tar
done
