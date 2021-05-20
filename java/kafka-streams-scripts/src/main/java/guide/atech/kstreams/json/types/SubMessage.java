package guide.atech.kstreams.json.types;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.commons.lang.builder.ToStringBuilder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "SubMessageCode"
})
public class SubMessage {

    @JsonProperty("SubMessageCode")
    private Integer SubMessageCode;

    @JsonProperty("SubMessageCode")
    public Integer getSubMessageCode() {
        return SubMessageCode;
    }

    @JsonProperty("SubMessageCode")
    public void setSubMessageCode(Integer subMessageCode) {
        this.SubMessageCode = subMessageCode;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("messageCode", SubMessageCode)
                .toString();
    }
}
