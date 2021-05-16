package guide.atech.producers.json.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

@NoArgsConstructor
@Slf4j
public class JsonSerializer<T> implements Serializer<T> {

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void configure(Map configs, boolean isKey) {
        Serializer.super.configure(configs, isKey);
        // Nothing to configure
    }

    @Override
    public byte[] serialize(String s, T data) {

        if (null == data) {
            return new byte[0];
        }
        try {
            return mapper.writeValueAsBytes(data);
        } catch (Exception e) {
            log.error("Message Serialization failed with Exception e {}", e.getLocalizedMessage());
            throw new SerializationException("Error Serializing JSON Message", e);
        }
    }

    @Override
    public void close() {
        Serializer.super.close();
    }
}
