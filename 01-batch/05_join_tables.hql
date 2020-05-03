-- MPCS 53014 Big Data Application Architecture
-- Jonathan Tan
-- Batch Layer File 05: Join tables
-- Batch data is joined by node_id but API updates identify nodes by node_vsn

-- Create new ORC table to hold joined data
DROP TABLE IF EXISTS jonathantan_node_data;
CREATE TABLE jonathantan_node_data (
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
) STORED AS ORC;

-- Join and insert data
INSERT OVERWRITE TABLE jonathantan_node_data
    SELECT
        s.time_stamp,
        s.node_id,
        n.vsn AS node_vsn,
        n.address,
        n.lat,
        n.lon,
        n.description,
        s.subsystem,
        s.sensor,
        s.parameter,
        s.value_drf AS value
    FROM jonathantan_sensor_data s JOIN jonathantan_nodes n
    ON s.node_id = n.node_id;
