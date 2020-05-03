-- MPCS 53014 Big Data Application Architecture
-- Jonathan Tan
-- Batch Layer File 04: Create supporting tables for node data

-- Create external table from raw CSV files
DROP TABLE IF EXISTS jonathantan_nodes_csv;
CREATE EXTERNAL TABLE jonathantan_nodes_csv (
    node_id STRING,
    project_id STRING,
    vsn STRING,
    address STRING,
    lat DECIMAL(9,6),
    lon DECIMAL(9,6),
    description STRING,
    start_timestamp TIMESTAMP,
    end_timestamp TIMESTAMP
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = "\,",
   "quoteChar"     = "\"",
   "timestamp.formats" = "yyyy/MM/dd HH:mm:ss"
)
STORED AS TEXTFILE
  LOCATION '/jonathantan/data/nodes';

-- Create ORC table to hold node data
DROP TABLE IF EXISTS jonathantan_nodes;
CREATE TABLE jonathantan_nodes (
    node_id STRING,
    project_id STRING,
    vsn STRING,
    address STRING,
    lat DECIMAL(9,6),
    lon DECIMAL(9,6),
    description STRING,
    start_timestamp TIMESTAMP,
    end_timestamp TIMESTAMP
)
STORED AS ORC;

-- Store node data as ORC
INSERT OVERWRITE TABLE jonathantan_nodes
    SELECT
        node_id,
        project_id,
        vsn,
        address,
        lat,
        lon,
        description,
        cast(translate(start_timestamp, '/', '-') AS TIMESTAMP),
        cast(translate(end_timestamp, '/', '-') AS TIMESTAMP)
    FROM jonathantan_nodes_csv
    WHERE node_id IS NOT NULL
        AND node_id != "node_id";
