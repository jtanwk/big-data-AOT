-- MPCS 53014 Big Data Application Architecture
-- Jonathan Tan
-- Batch Layer File 03: Create master dataset

-- Create external table from raw CSV files
DROP TABLE IF EXISTS jonathantan_sensor_data_csv;
CREATE EXTERNAL TABLE jonathantan_sensor_data_csv (
    time_stamp TIMESTAMP,
    node_id STRING,
    subsystem STRING,
    sensor STRING,
    parameter STRING,
    value_raw FLOAT,
    value_drf FLOAT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = "\,",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE
  LOCATION '/jonathantan/data/sensor_data';

-- Create ORC table to hold master data
DROP TABLE IF EXISTS jonathantan_sensor_data;
CREATE TABLE jonathantan_sensor_data (
    time_stamp TIMESTAMP,
    node_id STRING,
    subsystem STRING,
    sensor STRING,
    parameter STRING,
    value_raw FLOAT,
    value_drf FLOAT
)
STORED AS ORC;

-- Store master data as ORC
INSERT OVERWRITE TABLE jonathantan_sensor_data
    SELECT
        cast(translate(time_stamp, '/', '-') AS TIMESTAMP),
        node_id,
        subsystem,
        sensor,
        parameter,
        value_raw,
        value_drf
    FROM jonathantan_sensor_data_csv
    WHERE node_id IS NOT NULL
        AND subsystem IS NOT NULL
        AND sensor IS NOT NULL
        AND parameter IS NOT NULL
        AND node_id != "node_id";
