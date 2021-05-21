
package guide.atech.schema.json;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.commons.lang.builder.ToStringBuilder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "itemCode",
    "ItemDescription",
    "ItemPrice",
    "ItemQuantity",
    "TotalValue"
})
public class LineItem {

    @JsonProperty("itemCode")
    private String itemCode;
    @JsonProperty("ItemDescription")
    private String itemDescription;
    @JsonProperty("ItemPrice")
    private Double itemPrice;
    @JsonProperty("ItemQuantity")
    private Integer itemQuantity;
    @JsonProperty("TotalValue")
    private Double totalValue;

    @JsonProperty("itemCode")
    public String getItemCode() {
        return itemCode;
    }

    @JsonProperty("itemCode")
    public void setItemCode(String itemCode) {
        this.itemCode = itemCode;
    }

    public LineItem withItemCode(String itemCode) {
        this.itemCode = itemCode;
        return this;
    }

    @JsonProperty("ItemDescription")
    public String getItemDescription() {
        return itemDescription;
    }

    @JsonProperty("ItemDescription")
    public void setItemDescription(String itemDescription) {
        this.itemDescription = itemDescription;
    }

    public LineItem withItemDescription(String itemDescription) {
        this.itemDescription = itemDescription;
        return this;
    }

    @JsonProperty("ItemPrice")
    public Double getItemPrice() {
        return itemPrice;
    }

    @JsonProperty("ItemPrice")
    public void setItemPrice(Double itemPrice) {
        this.itemPrice = itemPrice;
    }

    public LineItem withItemPrice(Double itemPrice) {
        this.itemPrice = itemPrice;
        return this;
    }

    @JsonProperty("ItemQuantity")
    public Integer getItemQuantity() {
        return itemQuantity;
    }

    @JsonProperty("ItemQuantity")
    public void setItemQuantity(Integer itemQuantity) {
        this.itemQuantity = itemQuantity;
    }

    public LineItem withItemQuantity(Integer itemQuantity) {
        this.itemQuantity = itemQuantity;
        return this;
    }

    @JsonProperty("TotalValue")
    public Double getTotalValue() {
        return totalValue;
    }

    @JsonProperty("TotalValue")
    public void setTotalValue(Double totalValue) {
        this.totalValue = totalValue;
    }

    public LineItem withTotalValue(Double totalValue) {
        this.totalValue = totalValue;
        return this;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("itemCode", itemCode).append("itemDescription", itemDescription).append("itemPrice", itemPrice).append("itemQuantity", itemQuantity).append("totalValue", totalValue).toString();
    }

}
