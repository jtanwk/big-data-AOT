-- MPCS 53014 Big Data Application Architecture
-- Jonathan Tan
-- Serving Layer File 01: Recomputing hourly views

DROP TABLE IF EXISTS jonathantan_weather_this_hour;
CREATE EXTERNAL TABLE jonathantan_weather_this_hour (
    node_vsn STRING,
    month INT,
    day INT,
    temp_today FLOAT,
    hum_today FLOAT,
    temp_this_hour FLOAT,
    hum_this_hour FLOAT,
    temp_latest BIGINT,
    hum_latest BIGINT
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,stats:month,stats:day,stats:temp_today,stats:hum_today,stats:temp_this_hour,stats:hum_this_hour,stats:temp_latest#b,stats:hum_latest#b')
TBLPROPERTIES ('hbase.table.name' = 'jonathantan_weather_this_hour');

INSERT OVERWRITE TABLE jonathantan_weather_this_hour
    SELECT
        node_vsn,
        month(time_stamp) AS month,
        day(time_stamp) AS day,
        AVG(IF(parameter = 'temperature', value, NULL)) AS temp_today,
        AVG(IF(parameter = 'humidity', value, NULL)) AS hum_today,
        AVG(IF(parameter = 'temperature'
            AND hour(time_stamp) = hour(from_utc_timestamp(current_timestamp, "CST")),
            value, NULL)) AS temp_this_hour,
        AVG(IF(parameter = 'humidity'
            AND hour(time_stamp) = hour(from_utc_timestamp(current_timestamp, "CST")),
            value, NULL)) AS hum_this_hour,
        AVG(IF(parameter = 'temperature', value, NULL)) AS temp_latest,
        AVG(IF(parameter = 'humidity', value, NULL)) AS hum_latest
    FROM jonathantan_node_data
    WHERE month(time_stamp) = month(from_utc_timestamp(current_timestamp, "CST"))
        AND day(time_stamp) = day(from_utc_timestamp(current_timestamp, "CST"))
    GROUP BY node_vsn, month(time_stamp), day(time_stamp);
