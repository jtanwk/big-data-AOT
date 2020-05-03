#!/bin/bash

# hive_recompute_hourly.sh
# Runs hourly

echo "`date` Running hourly Hive script"
hive -f /home/jonathantan/project/serving/01_recompute_hourly.hql
