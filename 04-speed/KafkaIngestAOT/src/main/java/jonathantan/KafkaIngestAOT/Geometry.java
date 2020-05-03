
package jonathantan.KafkaIngestAOT;

import java.util.List;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "type",
    "crs",
    "coordinates"
})
public class Geometry {

    @JsonProperty("type")
    private String type;
    @JsonProperty("crs")
    private Crs crs;
    @JsonProperty("coordinates")
    private List<Double> coordinates = null;

    @JsonProperty("type")
    public String getType() {
        return type;
    }

    @JsonProperty("type")
    public void setType(String type) {
        this.type = type;
    }

    @JsonProperty("crs")
    public Crs getCrs() {
        return crs;
    }

    @JsonProperty("crs")
    public void setCrs(Crs crs) {
        this.crs = crs;
    }

    @JsonProperty("coordinates")
    public List<Double> getCoordinates() {
        return coordinates;
    }

    @JsonProperty("coordinates")
    public void setCoordinates(List<Double> coordinates) {
        this.coordinates = coordinates;
    }

}
