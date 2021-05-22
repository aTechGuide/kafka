package guide.atech.producers.builder;

import guide.atech.schema.avro.DeliveryAddress;
import guide.atech.schema.avro.LineItem;
import guide.atech.schema.avro.PosInvoice;
import java.util.Calendar;
import java.util.Collections;
import java.util.Random;

public class POSBuilder {

    private POSBuilder() {}

    private static final Random random = new Random();

    public static PosInvoice nextInvoice() {

        return PosInvoice.newBuilder()
                .setInvoiceNumber("Invoice Number-" + random.nextInt(10))
                .setCreatedTime(Calendar.getInstance().getTimeInMillis())
                .setTotalAmount(random.nextDouble())
                .setNumberOfItems(random.nextInt(5))
                .setDeliveryAddress(DeliveryAddress.newBuilder()
                        .setAddressLine("Address Line-" + random.nextInt(100))
                        .setCity("City-" + random.nextInt(15))
                        .setState("State-" + random.nextInt(15))
                        .setContactNumber("99999999" + random.nextInt(20))
                        .build())
                .setInvoiceLineItems(Collections.singletonList(LineItem.newBuilder()
                        .setItemCode("ItemCode-" + random.nextInt(20))
                        .setItemDescription("ItemDesc-" + random.nextInt(22))
                        .setItemPrice(random.nextDouble())
                        .setItemQty(random.nextInt(20))
                        .setTotalValue(random.nextDouble())
                        .build()))
                .build();
    }
}
