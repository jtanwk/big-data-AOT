
package jonathantan.KafkaIngestAOT;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "previous",
    "next",
    "current"
})
public class Links {

    @JsonProperty("previous")
    private Object previous;
    @JsonProperty("next")
    private String next;
    @JsonProperty("current")
    private String current;

    @JsonProperty("previous")
    public Object getPrevious() {
        return previous;
    }

    @JsonProperty("previous")
    public void setPrevious(Object previous) {
        this.previous = previous;
    }

    @JsonProperty("next")
    public String getNext() {
        return next;
    }

    @JsonProperty("next")
    public void setNext(String next) {
        this.next = next;
    }

    @JsonProperty("current")
    public String getCurrent() {
        return current;
    }

    @JsonProperty("current")
    public void setCurrent(String current) {
        this.current = current;
    }

}
