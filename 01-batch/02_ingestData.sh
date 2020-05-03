#!/bin/bash

# 02_ingestData.sh
# 1. Untars the tar files
# 2. Unzips the internal data file and puts them into HDFS
# 3. Removes the tar and untarred data

for name in *.tar; do
    echo "Processing $name"

    # Decompress data
    tar -xf $name
    cd ${name%.tar}
    gunzip data.csv.gz

    # Put data in HDFS
    mv data.csv ${name%.tar}_data.csv
    mv nodes.csv ${name%.tar}_nodes.csv
    echo "    Uploading sensor data to HDFS"
    hdfs dfs -put ${name%.tar}_data.csv /jonathantan/data/sensor_data/${name%.tar}.csv
    # echo "    Uploading node list to HDFS"
    # hdfs dfs -put ${name%.tar}_nodes.csv /jonathantan/data/nodes/${name%.tar}_nodes.csv

    # Cleanup
    cd ..
    rm -r ${name%.tar}
    rm $name

done
