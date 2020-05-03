-- MPCS 53014 Big Data Application Architecture
-- Jonathan Tan
-- Batch Ingestion File 02: Convert new data to ORC

-- Create external table from raw CSV files
DROP TABLE IF EXISTS jonathantan_temp_csv;
CREATE EXTERNAL TABLE jonathantan_temp_csv (
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
  LOCATION '/jonathantan/data/new_data';

-- Create ORC table to hold master data
DROP TABLE IF EXISTS jonathantan_temp;
CREATE TABLE jonathantan_temp (
    time_stamp TIMESTAMP,
    node_id STRING,
    node_vsn STRING,
    address STRING,
    lat DECIMAL(9,6),
    lon DECIMAL(9,6),
    description STRING,
    subsystem STRING,
    sensor STRING,
    parameter STRING,
    value FLOAT
)
STORED AS ORC;

-- Store master data as ORC
INSERT OVERWRITE TABLE jonathantan_temp
    SELECT
        cast(translate(t.time_stamp, '/', '-') AS TIMESTAMP),
        t.node_id,
        n.vsn AS node_vsn,
        n.address,
        n.lat,
        n.lon,
        n.description,
        t.subsystem,
        t.sensor,
        t.parameter,
        t.value_drf AS value
    FROM jonathantan_temp_csv t JOIN jonathantan_nodes n
        ON t.node_id = n.node_id
    WHERE t.node_id IS NOT NULL
        AND t.subsystem IS NOT NULL
        AND t.sensor IS NOT NULL
        AND t.parameter IS NOT NULL
        AND t.node_id != "node_id";
