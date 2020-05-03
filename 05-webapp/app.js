'use strict';
const http = require('http');
var assert = require('assert');
const express= require('express');
const app = express();
const mustache = require('mustache');
const filesystem = require('fs');
const url = require('url');
const hbase = require('hbase-rpc-client');
const BigIntBuffer = require('bigint-buffer');

const hostname = '127.0.0.1';
const port = 3733;

var client = hbase({
    zookeeperHosts: ["mpcs53014c10-m-6-20191016152730.us-central1-a.c.mpcs53014-2019.internal:2181"],
    zookeeperRoot: "/hbase-unsecure"
});

client.on('error', function(err) {
  console.log(err)
})

app.use(express.static('public'));
app.get('/nodes.html', function (req, res) {
    const node_id = req.query['node'];
    const get = new hbase.Get(node_id);

    // Get average weather readings for today
    client.get("jonathantan_weather_this_hour", get, function(err, row) {

    	assert.ok(!err, `get returned an error: #{err}`);
    	if (!row) {
    	    res.send("<html><body>No data found.</body></html>");
    	    return;
	    }
        console.log(row);

    	var template = filesystem.readFileSync("result.mustache").toString();
    	var html = mustache.render(template, {
    	    node : req.query['node'],
            mean_temp_today : parseInt(row.cols["stats:temp_today"].value.toString()).toFixed(0),
            mean_humid_today : parseInt(row.cols["stats:hum_today"].value.toString()).toFixed(0),
            mean_temp_this_hour : parseInt(row.cols["stats:temp_this_hour"].value.toString()).toFixed(0),
            mean_humid_this_hour : parseInt(row.cols["stats:hum_this_hour"].value.toString()).toFixed(0),
            latest_temp : parseInt(row.cols["stats:temp_latest"].value.toString("hex"), 16).toFixed(0),
            latest_hum : parseInt(row.cols["stats:hum_latest"].value.toString("hex"), 16).toFixed(0),
            temp_diff : (parseInt(row.cols["stats:temp_latest"].value.toString("hex"), 16) -
                        parseInt(row.cols["stats:temp_this_hour"].value.toString())).toFixed(0),
            hum_diff :  (parseInt(row.cols["stats:hum_latest"].value.toString("hex"), 16) -
                        parseInt(row.cols["stats:hum_this_hour"].value.toString())).toFixed(0)
    	});

        res.send(html);
    });
});

app.listen(port);
