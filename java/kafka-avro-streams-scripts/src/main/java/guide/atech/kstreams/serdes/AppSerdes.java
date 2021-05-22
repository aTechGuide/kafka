package guide.atech.kstreams.serdes;

import guide.atech.schema.avro.PosInvoice;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import java.util.HashMap;
import java.util.Map;

/**
 * Ref
 * - https://docs.confluent.io/platform/current/streams/developer-guide/datatypes.html
 */
public class AppSerdes extends Serdes {

    public static Serde<PosInvoice> PosInvoice() {

        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put("schema.registry.url", "http://localhost:8081");

        // We are using Confluent's Specific Avro serde which would internally use Avro serializer and deserializer.
        Serde<PosInvoice> serde = new SpecificAvroSerde<>();
        serde.configure(serdeConfigs, false);

        return serde;
    }
}
