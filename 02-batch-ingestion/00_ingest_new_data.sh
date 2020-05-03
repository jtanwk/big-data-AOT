#!/bin/bash

# 00_ingest_new_data.sh
# Master control script that is called by cron monthly

# 1. Download, untar, ingest new data to HDFS
sh 01_download_bulk_data.sh

# 2. Store new data in ORC format
hive -f 02_convert_to_orc.hql

# 3. Move raw ORC files to master dataset directory
sh 03_move_orc_files.sh

# 4. Delete temporary tables created in step 2
hive -f 04_delete_temp_tables.hql
