
package guide.atech.kstreams.json2pojo;

import java.util.ArrayList;
import java.util.List;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.commons.lang.builder.ToStringBuilder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "InvoiceNumber",
    "CreatedTime",
    "CustomCardNo",
    "TotalAmount",
    "NumberOfItem",
    "PaymentMethod",
    "InvoiceLineItems"
})
public class Invoice {

    @JsonProperty("InvoiceNumber")
    private String invoiceNumber;
    @JsonProperty("CreatedTime")
    private Long createdTime;
    @JsonProperty("CustomCardNo")
    private String customCardNo;
    @JsonProperty("TotalAmount")
    private Double totalAmount;
    @JsonProperty("NumberOfItem")
    private Integer numberOfItem;
    @JsonProperty("PaymentMethod")
    private String paymentMethod;
    @JsonProperty("InvoiceLineItems")
    private List<LineItem> invoiceLineItems = new ArrayList<LineItem>();

    @JsonProperty("InvoiceNumber")
    public String getInvoiceNumber() {
        return invoiceNumber;
    }

    @JsonProperty("InvoiceNumber")
    public void setInvoiceNumber(String invoiceNumber) {
        this.invoiceNumber = invoiceNumber;
    }

    public Invoice withInvoiceNumber(String invoiceNumber) {
        this.invoiceNumber = invoiceNumber;
        return this;
    }

    @JsonProperty("CreatedTime")
    public Long getCreatedTime() {
        return createdTime;
    }

    @JsonProperty("CreatedTime")
    public void setCreatedTime(Long createdTime) {
        this.createdTime = createdTime;
    }

    public Invoice withCreatedTime(Long createdTime) {
        this.createdTime = createdTime;
        return this;
    }

    @JsonProperty("CustomCardNo")
    public String getCustomCardNo() {
        return customCardNo;
    }

    @JsonProperty("CustomCardNo")
    public void setCustomCardNo(String customCardNo) {
        this.customCardNo = customCardNo;
    }

    public Invoice withCustomCardNo(String customCardNo) {
        this.customCardNo = customCardNo;
        return this;
    }

    @JsonProperty("TotalAmount")
    public Double getTotalAmount() {
        return totalAmount;
    }

    @JsonProperty("TotalAmount")
    public void setTotalAmount(Double totalAmount) {
        this.totalAmount = totalAmount;
    }

    public Invoice withTotalAmount(Double totalAmount) {
        this.totalAmount = totalAmount;
        return this;
    }

    @JsonProperty("NumberOfItem")
    public Integer getNumberOfItem() {
        return numberOfItem;
    }

    @JsonProperty("NumberOfItem")
    public void setNumberOfItem(Integer numberOfItem) {
        this.numberOfItem = numberOfItem;
    }

    public Invoice withNumberOfItem(Integer numberOfItem) {
        this.numberOfItem = numberOfItem;
        return this;
    }

    @JsonProperty("PaymentMethod")
    public String getPaymentMethod() {
        return paymentMethod;
    }

    @JsonProperty("PaymentMethod")
    public void setPaymentMethod(String paymentMethod) {
        this.paymentMethod = paymentMethod;
    }

    public Invoice withPaymentMethod(String paymentMethod) {
        this.paymentMethod = paymentMethod;
        return this;
    }

    @JsonProperty("InvoiceLineItems")
    public List<LineItem> getInvoiceLineItems() {
        return invoiceLineItems;
    }

    @JsonProperty("InvoiceLineItems")
    public void setInvoiceLineItems(List<LineItem> invoiceLineItems) {
        this.invoiceLineItems = invoiceLineItems;
    }

    public Invoice withInvoiceLineItems(List<LineItem> invoiceLineItems) {
        this.invoiceLineItems = invoiceLineItems;
        return this;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("invoiceNumber", invoiceNumber).append("createdTime", createdTime).append("customCardNo", customCardNo).append("totalAmount", totalAmount).append("numberOfItem", numberOfItem).append("paymentMethod", paymentMethod).append("invoiceLineItems", invoiceLineItems).toString();
    }

}
