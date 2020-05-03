
package jonathantan.KafkaIngestAOT;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "type",
    "properties"
})
public class Crs {

    @JsonProperty("type")
    private String type;
    @JsonProperty("properties")
    private CrsProperties properties;

    @JsonProperty("type")
    public String getType() {
        return type;
    }

    @JsonProperty("type")
    public void setType(String type) {
        this.type = type;
    }

    @JsonProperty("properties")
    public CrsProperties getProperties() {
        return properties;
    }

    @JsonProperty("properties")
    public void setProperties(CrsProperties properties) {
        this.properties = properties;
    }

}
