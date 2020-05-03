
package jonathantan.KafkaIngestAOT;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "value",
    "uom",
    "timestamp",
    "sensor_path",
    "node_vsn",
    "location"
})
public class Datum {

    @JsonProperty("value")
    private Double value;
    @JsonProperty("uom")
    private String uom;
    @JsonProperty("timestamp")
    private String timestamp;
    @JsonProperty("sensor_path")
    private String sensorPath;
    @JsonProperty("node_vsn")
    private String nodeVsn;
    @JsonProperty("location")
    private Location location;

    @JsonProperty("value")
    public Double getValue() {
        return value;
    }

    @JsonProperty("value")
    public void setValue(Double value) {
        this.value = value;
    }

    @JsonProperty("uom")
    public String getUom() {
        return uom;
    }

    @JsonProperty("uom")
    public void setUom(String uom) {
        this.uom = uom;
    }

    @JsonProperty("timestamp")
    public String getTimestamp() {
        return timestamp;
    }

    @JsonProperty("timestamp")
    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    @JsonProperty("sensor_path")
    public String getSensorPath() {
        return sensorPath;
    }

    @JsonProperty("sensor_path")
    public void setSensorPath(String sensorPath) {
        this.sensorPath = sensorPath;
    }

    @JsonProperty("node_vsn")
    public String getNodeVsn() {
        return nodeVsn;
    }

    @JsonProperty("node_vsn")
    public void setNodeVsn(String nodeVsn) {
        this.nodeVsn = nodeVsn;
    }

    @JsonProperty("location")
    public Location getLocation() {
        return location;
    }

    @JsonProperty("location")
    public void setLocation(Location location) {
        this.location = location;
    }

}
