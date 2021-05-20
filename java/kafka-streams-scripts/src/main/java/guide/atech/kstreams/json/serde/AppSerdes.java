package guide.atech.kstreams.json.serde;

import guide.atech.kstreams.json.types.Message;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import java.util.HashMap;
import java.util.Map;

public class AppSerdes extends Serdes {

    static final class MessageSerde extends Serdes.WrapperSerde<Message> {
        public MessageSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>());
        }
    }

    public static Serde<Message> Message() {

        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, Message.class);

        MessageSerde serde = new MessageSerde();
        serde.configure(serdeConfigs, false);

        return serde;
    }
}
