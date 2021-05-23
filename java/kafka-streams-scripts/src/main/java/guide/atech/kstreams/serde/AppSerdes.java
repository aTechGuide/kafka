package guide.atech.kstreams.serde;

import guide.atech.kstreams.json.types.Message;
import guide.atech.schema.json.Notification;
import guide.atech.schema.json.PosInvoice;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import java.util.HashMap;
import java.util.Map;

public class AppSerdes extends Serdes {

    // Message Serde
    static final class MessageSerde extends Serdes.WrapperSerde<Message> {
        public MessageSerde() {
            super(new JsonSerializer<Message>(), new JsonDeserializer<Message>());
        }
    }

    public static Serde<Message> Message() {

        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, Message.class);

        MessageSerde serde = new MessageSerde();
        serde.configure(serdeConfigs, false);

        return serde;
    }

    // POS Invoice Serde
    static final class PosInvoiceSerde extends Serdes.WrapperSerde<PosInvoice> {
        public PosInvoiceSerde() {
            super(new JsonSerializer<PosInvoice>(), new JsonDeserializer<PosInvoice>());
        }
    }

    public static Serde<PosInvoice> PosInvoice() {

        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, Message.class);

        PosInvoiceSerde serde = new PosInvoiceSerde();
        serde.configure(serdeConfigs, false);

        return serde;
    }

    // Notification Serde
    static final class NotificationSerde extends Serdes.WrapperSerde<Notification> {
        public NotificationSerde() {
            super(new JsonSerializer<Notification>(), new JsonDeserializer<Notification>());
        }
    }

    public static Serde<Notification> Notification() {

        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, Notification.class);

        NotificationSerde serde = new NotificationSerde();
        serde.configure(serdeConfigs, false);

        return serde;
    }
}
