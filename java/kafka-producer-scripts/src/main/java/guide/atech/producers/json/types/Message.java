package guide.atech.producers.json.types;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.commons.lang.builder.ToStringBuilder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "MessageCode",
        "MessagePrice",
        "MessageQuantity"
})
public class Message {

    @JsonProperty("MessageCode")
    private String messageCode;

    @JsonProperty("MessagePrice")
    private Double messagePrice;

    @JsonProperty("MessageQuantity")
    private Integer messageQuantity;

    @JsonProperty("MessageCode")
    public String getMessageCode() {
        return messageCode;
    }

    @JsonProperty("MessageCode")
    public void setMessageCode(String messageCode) {
        this.messageCode = messageCode;
    }

    @JsonProperty("MessagePrice")
    public Double getMessagePrice() {
        return messagePrice;
    }

    @JsonProperty("MessagePrice")
    public void setMessagePrice(Double messagePrice) {
        this.messagePrice = messagePrice;
    }

    @JsonProperty("MessageQuantity")
    public Integer getMessageQuantity() {
        return messageQuantity;
    }

    @JsonProperty("MessageQuantity")
    public void setMessageQuantity(Integer messageQuantity) {
        this.messageQuantity = messageQuantity;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("messageCode", messageCode)
                .append("messagePrice", messagePrice)
                .append("messageQuantity", messageQuantity)
                .toString();
    }
}
