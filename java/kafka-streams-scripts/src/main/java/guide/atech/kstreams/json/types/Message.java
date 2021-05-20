package guide.atech.kstreams.json.types;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.commons.lang.builder.ToStringBuilder;

import java.util.ArrayList;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "MessageCode",
        "MessageDescription",
        "SubMessages"
})
public class Message {

    @JsonProperty("MessageCode")
    private Integer messageCode;

    @JsonProperty("MessageDescription")
    private String messageDescription;

    @JsonProperty("SubMessages")
    private List<SubMessage> subMessages = new ArrayList<>();

    @JsonProperty("MessageCode")
    public Integer getMessageCode() {
        return messageCode;
    }

    @JsonProperty("MessageCode")
    public void setMessageCode(Integer messageCode) {
        this.messageCode = messageCode;
    }

    @JsonProperty("MessageDescription")
    public String getMessageDescription() {
        return messageDescription;
    }

    @JsonProperty("MessageDescription")
    public void setMessageDescription(String messageDescription) {
        this.messageDescription = messageDescription;
    }

    @JsonProperty("SubMessages")
    public List<SubMessage> getSubMessages() {
        return subMessages;
    }

    @JsonProperty("SubMessages")
    public void setSubMessages(List<SubMessage> subMessages) {
        this.subMessages = subMessages;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("messageCode", messageCode)
                .append("messagePrice", messageDescription)
                .append("subMessages", subMessages)
                .toString();
    }
}